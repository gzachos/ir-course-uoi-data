#!/usr/bin/env python3

import os
import sys
import time
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import threading
import random
from math import ceil


########################
# Function definitions #
########################


# Print message to STDERR.
def perror(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    sys.stderr.flush()


# Article titles starting with a '.' will be stored as hidden files.
# i.e. '.NET_Framework'. This function replaces leading '.' with '__dot__'.
def canonicalize(filename):
    filename = filename[:filename_max_size]
    if filename.startswith('.'):
        filename = '__dot__' + filename[1:]
    return filename


# Read seeds from text file.
def read_seeds():
    seeds = []
    try:
        f = open(seeds_filename, mode='r', encoding='utf-8')
        for line in f.readlines():
            seeds.append(line.strip())
        return seeds
    except OSError as ose:
        perror(seeds_filename + ': ' + ose.strerror)
        exit(ose.errno)


# Parses an HTML text and adds the hyperlinks currently not in the crawl
# frontier. Articles named {ISO,IEC,IEEE}_* and 802.* are excluded to support
# a larger  variety of articles as there are many variants of them.
# i.e. IEEE_802.11{ac,ad,af,ah,ai,ax,ay,be}
# Return: 1) True if no more hyperlinks need to be extracted
#         2) True if parsing was successful
def expand_frontier(html_text):
    limit_reached = False
    success = True
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        links = soup.find('div', id='mw-content-text').find_all('a')
        title = soup.find('h1', id="firstHeading").string
        for link in links:
            href = str(link.get('href'))
            path_tokens = href.strip('/').split('/')
            if href.startswith('/wiki/') and len(path_tokens) == 2 \
                    and not ('#' in href or ':' in href):
                if not href in crawl_frontier:
                    if len(crawl_frontier) == article_limit:
                        limit_reached = True
                        break
                    print('Adding   \'%s\' to frontier' % (href))
                    crawl_frontier.append(href)
    except:
        success = False
        perror('Error parsing article \'%s\' for hyperlinks' % (title))
    return limit_reached, success


# Download an article and parse it to extract more hyperlinks.
# Return: 1) True if no more hyperlinks need to be extracted
#         2) True if hyperlink extraction was successful
def extract_hrefs_from_article(href):
    download_attempts = 0
    limit_reached = success = False
    while download_attempts <= max_downld_retries:
        try:
            url = url_prefix + href
            print('Parsing \'%s\'' % (url))
            req = requests.get(url)
            if req.status_code != 200:
                raise Exception('Status code: ' + str(req.status_code))
            limit_reached, success = expand_frontier(req.text)
            break
        except Exception as e:
            perror('Error extracting hrefs from: \'%s\'' % (url))
            download_attempts += 1
            time.sleep(5)
    return limit_reached, success


# Build crawl frontier using input seeds.
def build_crawl_frontier(seeds):
    global crawl_frontier
    crawl_frontier = seeds
    webpages_parsed = 0

    for href in crawl_frontier:
        limit_reached, success = extract_hrefs_from_article(href)
        if success == True:
            webpages_parsed += 1
        if limit_reached == True:
            break

    return crawl_frontier, webpages_parsed


# Write URLs to urls.txt
def write_urls_tofile(article_hrefs):
    try:
        outfile = open(repo_path + 'urls.txt', mode='w', encoding='utf-8')
        for href in article_hrefs:
            outfile.write(href + '\n')
        outfile.close()
    except OSError as ose:
        perror('Cannot write \'urls.txt\': ' + ose.strerror)
        exit(ose.errno)


# Download article and save raw HTML file.
# Return number of downloaded articles (0 or 1).
def download_article(href):
    download_attempts = 0
    while download_attempts <= max_downld_retries:
        try:
            url = url_prefix + href
            filename = canonicalize(href.split('/')[-1]) + '.html'
            print('Downloading \'%s\' -> \'%s\'' % (url, filename))
            req = requests.get(url)
            if req.status_code != 200:
                raise RequestException()
            outfile = open(repo_path + filename, mode='w', encoding='utf-8')
            outfile.write(req.text)
            outfile.close()
            return 1   # Downloaded one article
        except RequestException as e:
            # perror(e)
            perror('Error downloading: \'%s\' [attempt %d/%d]' %
                    (url, download_attempts + 1, max_downld_retries + 1))
            if download_attempts == max_downld_retries:
                download_failures.append(url)
        except OSError as ose:
            perror('Error writing: \'%s\' -> \'%s\': %s [attempt %d/%d' %
                    (url, repo_path + filename, ose.strerror,
                        download_attempts + 1, max_downld_retries + 1))
            if download_attempts == max_downld_retries:
                write_failures.append(filename)
        download_attempts += 1
        time.sleep(1)
    return 0   # No article was stored


# Statically assign work to each thread.
def calculate_chunk(article_hrefs, tid, num_threads):
    num_hrefs = len(article_hrefs)
    chunksize = num_hrefs // num_threads
    remainder = num_hrefs % num_threads

    if remainder != 0:   # hrefs cannot be divided in equal chunks
        if chunksize == 0:   # num_hrefs < num_threads
            chunksize = 1
            lb = tid
            if tid >= num_hrefs:
                return (-1, -1)
        else:   # num_hrefs > num_threads
            # First remainder threads get one more href than the remaining
            if tid < remainder:
                chunksize += 1
                lb = tid * chunksize
            else:
                lb = remainder * (chunksize + 1) + (tid - remainder) * chunksize
    else:   # Chunksize is the same for all threads
        lb = tid * chunksize
    return (lb, lb + chunksize)


# Perform download of assigned chunk.
def download(article_hrefs, tid, num_threads):
    global total_downloads
    local_downloads = 0
    # Scrape articles assigned to me
    # print("TID %3d: [%d-%d)" % (tid, lb, ub))
    for href in article_hrefs:
        local_downloads += download_article(href)

    # Update total_downloads using synchronization to avoid race conditions
    # perror('down:tid: %d' % (local_downloads))
    tlock.acquire()
    total_downloads += local_downloads
    tlock.release()
    # print('Thread %3d is exiting...' % (tid))


def multithreaded_download(article_hrefs):
    thread_list = []
    # Create threads
    for i in range(num_threads):
        lb, ub = calculate_chunk(article_hrefs, i, num_threads)
        if lb == -1 and ub == -1:   # No work to be assigned
            continue
        arg_list = (article_hrefs[lb:ub], i, num_threads)
        thread = threading.Thread(target=download, args = arg_list)
        thread_list.append(thread)
        thread.start()
    # Join threads
    for thread in thread_list:
        thread.join()


def print_stats(webpages_parsed, frontier_build_time, download_time):
    download_fail_num = len(download_failures)
    write_fail_num = len(write_failures)
    failed_num = download_fail_num + write_fail_num
    success_num = total_downloads - failed_num
    print('\n################################ STATS ##########################################')
    print('Extracted %d hyperlinks from %d articles in %.2f minutes' %
            (article_limit, webpages_parsed, frontier_build_time/60))
    print('Downloaded %d/%d articles in %.2f minutes using %d threads [%d processors]'
            % (total_downloads, article_limit, download_time/60, num_threads, num_processors))
    if download_fail_num > 0:
        print('Failed to download %d webpages [%.4f%%]' %
                (download_fail_num, download_fail_num / article_limit * 100))
    if write_fail_num > 0:
        print('Failed to write %d HTML documents [%.4f%%]' %
                (write_fail_num, write_fail_num / total_downloads * 100))
    print('Removed %d/%d articles to drop article count to %d' %
            (num_removals+1, total_downloads, article_target))
    print('#################################################################################\n')


def print_failures():
    if len(download_failures) > 0:
        print('\nFailed to download the following webpages:')
        for i in range(len(download_failures)):
            print('%2d - %s' % (i+1, download_failures[i]))
    if len(write_failures) > 0:
        print('\nFailed to write the following HTML files:')
        for i in range(len(write_failures)):
            print(' %2d - %s' % (i+1, write_failures[i]))


def list_html_files():
    try:
        files = os.listdir(repo_path)
        html_files = [f for f in files if f.endswith('.html')]
        return html_files
    except Exception as e:
        perror('Cannot list files in directory: %s' % (repo_path))
        traceback.print_exc()
        exit(1)


def remove_file(filepath):
    try:
        os.unlink(filepath)
    except OSError as ose:
        perror('Cannot remove file \'%s\': %s' % (filepath, ose.strerror))
        exit(ose.errno)


# Remove redundant files to reach article_target
def remove_redundant_files():
    global num_removals
    html_files = list_html_files()
    num_removals = len(html_files) - article_target
    for i in range(num_removals):
        rand_file = random.choice(html_files)
        print("Removing redundant file: %3d - %s" % (i+1, rand_file))
        remove_file(repo_path + rand_file)


def main():
    seeds = read_seeds()
    t0 = time.time()
    article_hrefs, webpages_parsed = build_crawl_frontier(seeds)
    t1 = time.time()
    write_urls_tofile(article_hrefs)
    t2 = time.time()
    actual_downloads = multithreaded_download(article_hrefs)
    t3 = time.time()
    frontier_build_time = t1 - t0
    download_time = t3 - t2
    remove_redundant_files()
    print_failures()
    print_stats(webpages_parsed, frontier_build_time, download_time)


###############
# Global data #
###############
filename_max_size = 64
repo_path = './repository/'   # Where downloaded HTML files will be stored
url_prefix = 'https://en.wikipedia.org'
seeds_filename = 'crawler-seeds-extended.txt'   # Crawler seeds (extended list)
article_target = 100000 # Number of articles to download (>= 10)
# Number of articles to add in the crawler frontier is 0.5% more than
# article_target for redundancy reasons (i.e. bad hyperlinks).
article_limit = ceil(article_target * 1.005)
num_processors = os.cpu_count()
num_threads = num_processors * 4   # Number of threads used during downloading
max_downld_retries = 3   # How many times (at most) retry downloading an article
total_downloads = 0   # How many articles where downloaded by all threads
tlock = threading.Lock()   # Protects access to total_downloads
download_failures = []
write_failures = []


if __name__ == '__main__':
    main()


