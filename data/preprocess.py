#!/usr/bin/env python3

import os
import sys
import time
from bs4 import BeautifulSoup


########################
# Function definitions #
########################


# Print message to STDERR.
def perror(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    sys.stderr.flush()


def print_stats(preproc_time, article_count):
    parse_fail_num = len(parse_failures)
    write_fail_num = len(write_failures)
    failed_num = parse_fail_num + write_fail_num
    success_num = article_count - failed_num
    print('\n############################## STATS ##############################')
    print('Extracted text from %d HTML files in %.3f minutes' %
            (article_count - parse_fail_num, preproc_time / 60))
    print('Failed to parse %d HTML documents [%.2f%%]' %
            (parse_fail_num, parse_fail_num / article_count * 100))
    print('Failed to write %d TXT documents [%.2f%%]' %
            (write_fail_num, write_fail_num / article_count * 100))
    print('Succesfully extracted text from %d documents [%.2f%%]' %
            (success_num, success_num / article_count * 100))
    print('###################################################################\n')


def print_failures():
    if len(parse_failures) > 0:
        print('Failed to extract text from the following HTML files:')
        for fail in parse_failures:
            print(fail)
    if len(write_failures) > 0:
        print('\nFailed to write the following TXT files:')
        for fail in write_failures:
            print(fail)


def remove_file(filepath):
    try:
        os.unlink(filepath)
    except OSError as ose:
        perror('Cannot remove file \'%s\': %s' % (filepath, ose.strerror))
        exit(ose.errno)


def print_plain_text(dictionary):
    for key in dictionary:
        print('\n\n###############')
        print(key)
        print('###############')
        print(dictionary[key])


def write_plain_text(dictionary, target_filename, canonical_url):
    try:
        filepath = corpus_path + target_filename
        outfile = open(filepath, 'w')
        outfile.write(canonical_url)
        outfile.write(field_separator)
        first_key = True
        for key in dictionary:
            outfile.write(key)
            if first_key == True:
                first_key = False
                outfile.write(field_separator)
            outfile.write(dictionary[key])
    except:
        perror('\tCannot write \'%s\'' % (filepath))
        write_failures.append(target_filename)
        remove_file(filepath)


def parse_h1(h):
    hstr = ''
    for c in h.children:
        if c.string != None:
            hstr += c.string
    return hstr


def parse_h(h):
    for c in h.children:
        if c.has_attr('class') and 'mw-headline' in c.attrs['class']:
            if c.string != None:
                return c.string
            for sc in c.children:
                if sc.string != None:
                    return sc.string
                # print('h: ' + str(sc.name) + " '" + str(sc.string) + "' " + str(sc.string == None))
    return ''


def parse_html(content):
    string = ''
    # print('\n########################\n')
    if content.string != None:
        if content.name in ['sup', 'style', 'caption']: # caption is article title
            return ''
        # print(content.string)
        return content.string
    if content.name == None:
        return ''

    for c in content.children:
        string += parse_html(c)
    '''
    print(content.name)
    print('\nText:\n')
    print(content.get_text())
    print('\nString:\n')
    print(content.string)
    print('\nRaw:\n')
    print(content)
    '''
    return string


def parse_article(html_filename):
    plain_text = {}
    try:
        infile = open(repo_path + html_filename, 'r')
        soup = BeautifulSoup(infile, 'html5lib')
        # soup = BeautifulSoup(infile, 'html.parser')
        canonical_url = soup.head.find('link', rel='canonical').get('href')
        title = parse_h1(soup.body.find('h1', id='firstHeading'))
        # print(title)
        content = soup.find('div', id='mw-content-text').contents[0]
        # print(content.prettify())
        curr_heading = title
        plain_text[curr_heading] = ''
        for c in content.children:
            # print('\n########################\n')
            # print(c)
            # print('\n')
            if c.name == None:   # Ignore comments
                continue
            elif c.name == 'h2':
                curr_heading = parse_h(c)
                plain_text[curr_heading] = ''
                continue
            elif c.name in ['h3','h4','h5','h6']:
                plain_text[curr_heading] += '\n' + parse_h(c) + '\n'
                continue
            # print(c.attrs)
            if c.has_attr('id'):
                # Ignore table of contents
                if c.attrs['id'] == 'toc':
                    continue
            elif c.has_attr('class'):
                classes = c.attrs['class']
                if 'navbox' in classes: # Ignore navbox
                    continue
                if 'vcard' in classes or 'infobox' in classes:
                    continue   # TODO parse vcard, infobox
               # if 'reflist' in classes:
               #     continue   # TODO parse references
            string = parse_html(c)
            plain_text[curr_heading] += string
            # print(string)
        return plain_text, canonical_url
    except:
        perror('Cannot parse file: \'%s\'' % (html_filename))
        parse_failures.append(html_filename)
        return {}, canonical_url


def preprocess_files():
    article_count = 0
    try:
        files = os.listdir(repo_path)
        html_files = [f for f in files if f.endswith('.html')]
        for hf in html_files:
            article_count += 1
            #if 'RKM_code' in f:
            #if article_count == 424:
            if True:
                print('------------------------------------------------------')
                print(str(article_count) + ': ' + hf)
                dictionary, url = parse_article(hf)
                if dictionary != {} and url != None:
                    # print_plain_text(dictionary)
                    write_plain_text(dictionary, hf[:-5] + corpus_doc_suffix, url)
        return article_count
    except Exception as e:
        perror('Cannot list files in directory: %s' % (repo_path))
        exit(1)


def main():
    t0 = time.time()
    article_count = preprocess_files()
    t1 = time.time()
    preproc_time = t1 - t0
    print_failures()
    print_stats(preproc_time, article_count)



###############
# Global data #
###############
repo_path = './repository/'   # Where downloaded HTML files are stored
corpus_path = './corpus/'   # Where corpus (parsed) text files will be stored
corpus_doc_suffix = '.txt'
parse_failures = []   # filenames of HTML files that text wasn't extracted
write_failures = []   # filenames of TXT files that couldn't be stored to disk
field_separator = '\n\n\n'


if __name__ == '__main__':
    main()


