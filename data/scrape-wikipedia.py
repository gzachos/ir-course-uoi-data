#!/usr/bin/env python3

import os
import sys
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup


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
    global crawl_frontier
    if len(crawl_frontier) > 5000:
        return
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find(id='content').find_all('a')
    for link in links:
        href = str(link.get('href'))
        if href.startswith('/wiki/') and not ('#' in href or ':' in href):
            url = 'https://en.wikipedia.org' + href
            print('Adding \'%s\' to frontier' % (url))
            crawl_frontier.append(url)



def download(url, repo_path):
    global articles
    target_fname = repo_path + url.split('/')[-1] + '.html'
    try:
        outfile = open(target_fname, 'w')
        print('Downloading \'%s\' -> \'%s\'' % (url, target_fname))
        req = requests.get(url)
        if req.status_code != 200:
            raise RequestException()
        outfile.write(req.text)
    except RequestException:
        perror('Error downloading: \'%s\'' % (url))
        exit(1)
    except:
        perror('Error writing: \'%s\'' % (url))
        exit(1)
    expand_frontier(req.text)
    print(len(crawl_frontier))
    articles.append(Article(url, target_fname))
    return target_fname


def crawl(seeds):
    global articles, crawl_frontier
    repo_path = './repository/'
    crawl_frontier = seeds
    for url in crawl_frontier:
        download(url, repo_path)

    try:
        outfile = open(repo_path + 'urls.txt', 'w')
        for article in articles:
            outfile.write(article.url + "\n")
    except:
        perror('Cannot write \'urls.txt\'')
    finally:
        outfile.close()
        


# Global data
articles = []

seeds = read_seeds()
crawl(seeds)

print(len(crawl_frontier))
