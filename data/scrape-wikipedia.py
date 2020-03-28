#!/usr/bin/env python3

import os
import sys
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException


class Article:
    def __init__(self, url=None, filename=None):
        self.url = url
        self.filename = filename

    def __str__(self):
        return '%s : %s\n' % (self.url, self.filename)


def perror(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def read_seeds():
    seeds = []
    try:
        f = open('seeds.txt', 'r')
        for line in f.readlines():
            seeds.append(line.strip())
        return seeds
    except OSError as ose:
        perror('seeds.txt: ' + ose.strerror)
        exit(ose.errno)


def expand_frontier(html):
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find(id='content').find_all('a')
    for link in links:
        href = str(link.get('href'))
        path_tokens = href.strip('/').split('/')
        if href.startswith('/wiki/') and len(path_tokens) == 2 \
                and not ('#' in href or ':' in href) \
                and not ('ISO_' in href or 'IEEE_' in href) \
                and not ('802.' in href or 'IEC_' in href):
            url = 'https://en.wikipedia.org' + href
            if url in crawl_frontier:
                continue
            print('Adding \'%s\' to frontier' % (url))
            crawl_frontier.append(url)
            if len(crawl_frontier) == 5000:
                break


def scrape_article(article):
    try:
        url = article.url
        filepath = repo_path + article.filename
        outfile = open(filepath, 'w')
        print('Downloading \'%s\' -> \'%s\'' % (url, filepath))
        req = requests.get(url)
        if req.status_code != 200:
            raise RequestException()
        outfile.write(req.text)
        outfile.close()
    except RequestException:
        perror('Error downloading: \'%s\'' % (url))
        exit(1)
    except:
        perror('Error writing: \'%s\'' % (url))
        exit(1)


def crawl_article(url):
    try:
        print('Crawling \'%s\'' % (url))
        req = requests.get(url)
        if req.status_code != 200:
            raise Exception('Status code: ' + str(req.status_code))
    except Exception as e:
        perror('Error crawling: \'%s\'' % (url))
        exit(1)
    expand_frontier(req.text)


def crawl(seeds):
    global crawl_frontier
    crawl_frontier = seeds
    for url in crawl_frontier:
        crawl_article(url)
        if len(crawl_frontier) == 5000:
            break

    articles = []
    for url in crawl_frontier:
        target_fname = url.split('/')[-1] + '.html'
        articles.append(Article(url, target_fname))

    return articles


def write_urls_tofile(articles):
    try:
        outfile = open(repo_path + 'urls.txt', 'w')
        for article in articles:
            outfile.write(article.url + "\n")
        outfile.close()
    except:
        perror('Cannot write \'urls.txt\'')


def scrape(articles):
    for article in articles:
        scrape_article(article)


# Global data
repo_path = './repository/'

if __name__ == '__main__':
    seeds = read_seeds()
    articles = crawl(seeds)
    write_urls_tofile(articles)
    scrape(articles)

