#!/usr/bin/env python3

import os
import sys
import time
from bs4 import BeautifulSoup
import multiprocessing


########################
# Function definitions #
########################


# Print message to STDERR.
def perror(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    sys.stderr.flush()


def print_stats(preproc_time):
    parse_fail_num = len(parse_failures)
    write_fail_num = len(write_failures)
    failed_num = parse_fail_num + write_fail_num
    success_num = total_article_count - failed_num
    print('\n############################## STATS ##############################')
    print('Extracted text from %d HTML files in %.3f minutes' %
            (total_article_count - parse_fail_num, preproc_time / 60))
    print('Preprocessing was performed using %d processes' % (num_processes))
    if parse_fail_num != 0:
        print('Failed to parse %d HTML documents [%.2f%%]' %
                (parse_fail_num, parse_fail_num / total_article_count * 100))
    if write_fail_num != 0:
        print('Failed to write %d TXT documents [%.2f%%]' %
                (write_fail_num, write_fail_num / total_article_count * 100))
    if total_article_count != 0:
        print('Succesfully extracted text from %d documents [%.2f%%]' %
                (success_num, success_num / total_article_count * 100))
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
        outfile = open(filepath, mode='w', encoding='utf-8')
        outfile.write(canonical_url)
        outfile.write(field_separator)
        first_key = True
        for key in dictionary:
            outfile.write('\n' + key + '\n')
            if first_key == True:
                first_key = False
                outfile.write(field_separator)
            outfile.write(cleanup_section(dictionary[key]) + '\n')
    except:
        perror('\tCannot write \'%s\'' % (filepath))
        write_failures.append(target_filename)
        remove_file(filepath)


def cleanup_section(string):
    while '  ' in string:
        string = string.replace('  ', ' ')
    while ' \n' in string:
        string = string.replace(' \n', '\n')
    while '\n ' in string:
        string = string.replace('\n ', '\n')
    while '\n\n' in string:
        string = string.replace('\n\n', '\n')
    while ' ,' in string:
        string = string.replace(' ,', ',')
    while ' .' in string:
        string = string.replace(' .', '.')
    return string.strip()


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
        string += ' ' + parse_html(c)
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


def add_to_misc(c, misc, key, join_str):
    string = parse_html(c)
    if key in misc:
        misc[key] += join_str + string
    else:
        misc[key] = string


def search(classes, string, search_type='matches'):
    for cls in classes:
        if (search_type == 'startswith' and cls.startswith(string)) or \
                (search_type == 'contains' and string in cls) or \
                (search_type == 'matches' and string == cls):
            return True
    return False


# Returns dictionary of the form {heading: content} and the canonical url
def parse_article(html_filename):
    plain_text = {}
    misc = {}
    try:
        infile = open(repo_path + html_filename, mode='r', encoding='utf-8')
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
                plain_text[curr_heading] += parse_h(c)
                continue
            # print(c.attrs)
            elif c.has_attr('class'):
                classes = c.attrs['class']
                if search(classes, 'hatnote') == True or \
                        search(classes, 'noprint') == True or \
                        search(classes, 'haudio') == True:
                    continue
                if search(classes, 'navbox', 'contains') == True:
                    continue
                if search(classes, 'infobox') == True: # 'vcard'
                    add_to_misc(c, misc, '__infobox__', '')
                    continue
                if c.name == 'table':
                    if search(classes, 'box', 'startswith') == True:
                        continue
                if c.name == 'div':
                    if search(classes, 'toc', 'contains') == True:
                        continue
                    if search(classes, 'thumb') == True:
                        add_to_misc(c, misc, '__multimedia__', '\n')
                        continue
                    if search(classes, 'quotebox') == True:
                        add_to_misc(c, misc, '__quotes__', '\n')
                        continue
            string = parse_html(c)
            plain_text[curr_heading] += string
            #print(string)
        # Append misc sections like infobox/vcard etc. at the end.
        plain_text = dict(plain_text, **misc)
        return (plain_text, canonical_url)
    except:
        perror('Cannot parse file: \'%s\'' % (html_filename))
        parse_failures.append(html_filename)
        return ({}, canonical_url)


def preprocess_files(html_files, pid, queue):
    global total_article_count
    article_count = 0
    for hf in html_files:
        article_count += 1
        #if article_count == 100:
        #    break
        #if 'Bioinformatics' in hf:
        #if article_count == 11:
        if True:
            print('Process %2d: file: %4d - %s' % (pid, article_count, hf))
            dictionary, url = parse_article(hf)
            if dictionary != {} and url != None:
                # print_plain_text(dictionary)
                write_plain_text(dictionary, hf[:-5] + corpus_doc_suffix, url)

    # Update total_article_count using synchronization to avoid race conditions
    # Send the number of files processed to main thread
    queue.put(article_count)
    # print('Process %3d is exiting...' % (pid))


def calculate_chunk(html_files, pid, num_processes):
    num_files = len(html_files)
    chunksize = num_files // num_processes
    remainder = num_files % num_processes

    if remainder != 0:   # hrefs cannot be divided in equal chunks
        if chunksize == 0:   # num_files < num_processes
            chunksize = 1
            lb = pid
            if pid >= num_files:   # No work to be assigned 
                return (-1, -1)
        else:   # num_files > num_processes
            # First remainder processes get one more href than the remaining
            if pid < remainder:
                chunksize += 1
                lb = pid * chunksize
            else:
                lb = remainder * (chunksize + 1) + (pid - remainder) * chunksize
    else:   # Chunksize is the same for all processes
        lb = pid * chunksize
    return (lb, lb + chunksize)


def multiprocess_preprocessing(html_files):
    global total_article_count
    process_list = []
    # Create processes
    queue = multiprocessing.Queue()
    for i in range(num_processes):
        lb, ub = calculate_chunk(html_files, i, num_processes)
        if lb == -1 and ub == -1: # No work to be assigned
            continue
        arg_list = (html_files[lb:ub], i, queue)
        process = multiprocessing.Process(target=preprocess_files, args=arg_list)
        process_list.append(process)
        process.start()
    # Join processes
    for process in process_list:
        # Get the number of files processed by each thread
        total_article_count += queue.get()
        process.join()


def list_html_files():
    try:
        files = os.listdir(repo_path)
        html_files = [f for f in files if f.endswith('.html')]
        return html_files
    except Exception as e:
        perror('Cannot list files in directory: %s' % (repo_path))
        exit(1)


def main():
    html_files = list_html_files()
    t0 = time.time()
    multiprocess_preprocessing(html_files)
    t1 = time.time()
    preproc_time = t1 - t0
    print_failures()
    print_stats(preproc_time)


###############
# Global data #
###############
repo_path = './repository/'   # Where downloaded HTML files are stored
corpus_path = './corpus/'   # Where corpus (parsed) text files will be stored
corpus_doc_suffix = '.txt'
parse_failures = []   # filenames of HTML files that text wasn't extracted
write_failures = []   # filenames of TXT files that couldn't be stored to disk
field_separator = '\n\n'
num_processors = os.cpu_count()
num_processes = num_processors # Number of processes used during preprocessing
total_article_count = 0   # How many articles where preprocessed by all processes


if __name__ == '__main__':
    main()


