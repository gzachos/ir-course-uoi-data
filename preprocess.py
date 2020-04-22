#!/usr/bin/env python3

import os
import sys
import time
from bs4 import BeautifulSoup, NavigableString, Comment
import multiprocessing
import traceback


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
        print('\nFailed to extract text from the following HTML files:')
        for i in range(len(parse_failures)):
            print('%2d - %s' % (i+1, parse_failures[i]))
    if len(write_failures) > 0:
        print('\nFailed to write the following TXT files:')
        for i in range(len(write_failures)):
            print(' %2d - %s' % (i+1, write_failures[i]))


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


# Write plain text to a virtual XML file. The format is named virtual
# because the output is not a valid XML but XML tags are only used as
# field separators. Only one XML tag can exist per line, without any
# other text.
def write_virtual_xml(dictionary, target_filename, canonical_url):
    try:
        filepath = corpus_path + target_filename
        outfile = open(filepath, mode='w', encoding='utf-8')
        outfile.write('<document>\n')
        outfile.write('<url>\n')
        outfile.write(canonical_url)
        outfile.write('\n</url>\n')
        first_key = True
        for key in dictionary:
            if first_key == True:
                first_key = False
                outfile.write('<title>\n')
                outfile.write(key)
                outfile.write('\n</title>\n')
            outfile.write('<section>\n')
            outfile.write('<heading>\n')
            outfile.write(key)
            outfile.write('\n</heading>\n')
            outfile.write('<content>\n')
            outfile.write(cleanup_section(dictionary[key]))
            outfile.write('\n</content>\n')
            outfile.write('</section>\n')
        outfile.write('</document>\n')
    except:
        perror('\tCannot write \'%s\'' % (filepath))
        traceback.print_exc()
        write_failures.append(target_filename)
        remove_file(filepath)


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
        traceback.print_exc()
        write_failures.append(target_filename)
        remove_file(filepath)


def cleanup_section(string):
    string = string.replace('\t', ' ')
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


# Retrieve alt text from images inside 'mwe-math-element's.
def get_img_alt_text(img):
    if img == None:
        return ''
    if img.has_attr('class'):
        img_classes = img.attrs['class']
        if find_in(img_classes, 'mwe-math-fallback-image',
                search_type='startswith'):
            if img.has_attr('alt'):
                string = img.attrs['alt']
                # TODO parse math?
                return string
    return ''


# Parse sup element while ignoring those that contain hrefs.
def parse_sup(c, level, in_infobox):
    string = parse_childrenof(c, level, ignore_hrefs=True, in_infobox=in_infobox)
    if string != '':
        string  = '^' + string
    return string


# Add string to corrsponding misc[key] using join_str to concatenate.
def add_to_misc(key, string, join_str):
    global misc
    if key in misc:
        misc[key] += join_str + string
    else:
        misc[key] = string


# Search classes and return True if string exists.
# search_type = {'matches', 'contains', 'startswith'}
def find_in(classes, string, search_type='matches'):
    for cls in classes:
        if (search_type == 'startswith' and cls.startswith(string)) or \
                (search_type == 'contains' and string in cls) or \
                (search_type == 'matches' and string == cls):
            return True
    return False


# Used to call parse_child() recursively.
def parse_childrenof(c, level, ignore_hrefs=False, in_infobox=False):
    string = ''
    for inner_c in c.children:
        string += parse_child(inner_c, level+1, ignore_hrefs, in_infobox)
    return string


# Recursively parse current element/node. level corresponds to
# the relative nesting level of the HTML tag, ignore_refs is
# used to implement <sup> parsing and in_infobox is used to
# parse HTML tags of infobox class.
def parse_child(c, level, ignore_hrefs=False, in_infobox=False):
    global curr_heading
    ########################################################
    # Segment A - Handle Comments and NavigableStrings     #
    ########################################################
    if isinstance(c, Comment):  # Subclass of NavigableString so keep first
        return ''
    if isinstance(c, NavigableString):
        return c.string
    ########################################################
    # Segment B - Handle everything that is ignored        #
    ########################################################
    if c.name == 'style':
        return ''
    if c.name == 'script':  # Never true due to HTML subset
        return ''
    if c.name == 'caption':
        return ''
    if c.name == 'a' and ignore_hrefs == True:
        return ''
    if c.has_attr('role'):
        roles = c.attrs['role']
        if 'note' in roles:
            return ''
        if 'presentation' in roles:
            return ''
        if 'navigation' in roles:
            return ''
    if c.has_attr('class'):
        classes = c.attrs['class']
        if find_in(classes, 'navbox', search_type='contains'):
            return ''
        if find_in(classes, 'noprint'):
            return ''
        if find_in(classes, 'haudio'):
            return ''
        if find_in(classes, 'mw-editsection'):
            return ''
        if find_in(classes, 'mw-cite-backlink'):
            return '' # ^ in reflist
        if c.name == 'div':
            if find_in(classes, 'toc'):
                return ''
    ########################################################
    # Segment C - Handle elements containing useful text   #
    ########################################################
    if c.name == 'h2':
        curr_heading = parse_childrenof(c, level, ignore_hrefs, in_infobox)
        plain_text[curr_heading] = ''
        return ''
    if c.name in ['h3','h4','h5','h6']:
        string = parse_childrenof(c, level, ignore_hrefs, in_infobox)
        plain_text[curr_heading] += string
        return string
    if c.name == 'blockquote':
        string = parse_childrenof(c, level, ignore_hrefs, in_infobox)
        add_to_misc('__quotes__', string, '\n')
        plain_text[curr_heading] += string
        return ''
    if c.name == 'tr':  # Put one table row per line
        string = parse_childrenof(c, level, ignore_hrefs, in_infobox)
        string = string.replace('\n', ' ')
        return string
    if c.name == 'th' and in_infobox == True:
        string = parse_childrenof(c, level, ignore_hrefs, in_infobox=True)
        return ' ' + string + ' '  # Add the missing spaces
    if c.has_attr('class'):
        classes = c.attrs['class']
        if c.name == 'div':
            if find_in(classes, 'quotebox'):
                string = parse_childrenof(c, level, ignore_hrefs, in_infobox)
                add_to_misc('__quotes__', string, '\n')
                return ''
        if find_in(classes, 'thumbcaption'):
            string = parse_childrenof(c, level, ignore_hrefs, in_infobox)
            add_to_misc('__multimedia__', string, '\n')
            return ''
        if find_in(classes, 'gallerytext'):
            string = parse_childrenof(c, level, ignore_hrefs, in_infobox)
            add_to_misc('__multimedia__', string, '\n')
            return ''
        if find_in(classes, 'infobox'):
            string = parse_childrenof(c, level, ignore_hrefs, in_infobox=True)
            add_to_misc('__infobox__', string, '\n')
            return ''
        if find_in(classes, 'mwe-math-element'):  # Math formulas
            img = c.find('img')
            return get_img_alt_text(img)
        if c.name == 'sup':  # Keep after check for noprint etc.
            if find_in(classes, 'reference'):
                return ''
            elif find_in(classes, 'plainlinks'):
                return ''
            else:
                return parse_sup(c, level, in_infobox)
        if c.name == 'sub':
            return '_' + parse_childrenof(c, level, ignore_hrefs, in_infobox)
        if c.name == 'table':
            if find_in(classes, 'clade'):  # Ignore cladograms!
                return ''
    else:  # Following elements have no classes
        if c.name == 'sup':  # Keep after check for noprint etc.
            return parse_sup(c, level, in_infobox)
        if c.name == 'sub':
            return '_' + parse_childrenof(c, level, ignore_hrefs, in_infobox)
    return parse_childrenof(c, level, ignore_hrefs, in_infobox)


# Returns dictionary of the form {heading: content} and the canonical url
def parse_article(html_filename):
    global plain_text, misc, curr_heading
    plain_text = {}
    misc = {}
    try:
        infile = open(repo_path + html_filename, mode='r', encoding='utf-8')
        soup = BeautifulSoup(infile, 'html5lib')
        canonical_url = soup.head.find('link', rel='canonical').get('href')
        title = parse_childrenof(soup.body.find('h1', id='firstHeading'), level=0)
        content = soup.find('div', id='mw-content-text').contents[0]
        curr_heading = title
        plain_text[curr_heading] = ''
        for c in content.children:
            plain_text[curr_heading] += parse_child(c, level = 0)
        # Append misc sections like infobox/vcard etc. at the end.
        plain_text = dict(plain_text, **misc)
        return (plain_text, canonical_url)
    except:
        perror('Cannot parse file: \'%s\'' % (html_filename))
        traceback.print_exc()
        parse_failures.append(html_filename)
        return ({}, None)


def preprocess_files(html_files, pid, queue):
    article_count = 0
    for hf in html_files:
        article_count += 1
        print('Process %2d: file: %4d - %s' % (pid, article_count, hf))
        dictionary, url = parse_article(hf)
        if dictionary != {} and url != None:
            #print_plain_text(dictionary)
            write_plain_text(dictionary, hf[:-5] + corpus_doc_suffix, url)
            # write_virtual_xml(dictionary, hf[:-5] + corpus_doc_suffix_xml, url)

    # Send to main thread the number of files processed and the
    # filenames of the HTML files that couldn't be parsed or written.
    queue.put((article_count, parse_failures, write_failures))
    # print('Process %3d is exiting...' % (pid))


def calculate_chunk(html_files, pid, num_processes):
    num_files = len(html_files)
    chunksize = num_files // num_processes
    remainder = num_files % num_processes

    if remainder != 0:  # hrefs cannot be divided in equal chunks
        if chunksize == 0:  # num_files < num_processes
            chunksize = 1
            lb = pid
            if pid >= num_files:  # No work to be assigned
                return (-1, -1)
        else:  # num_files > num_processes
            # First remainder processes get one more href than the remaining
            if pid < remainder:
                chunksize += 1
                lb = pid * chunksize
            else:
                lb = remainder * (chunksize + 1) + (pid - remainder) * chunksize
    else:  # Chunksize is the same for all processes
        lb = pid * chunksize
    return (lb, lb + chunksize)


def multiprocess_preprocessing(html_files):
    global total_article_count, parse_failures, write_failures
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
        article_count, local_parse_failures, local_write_failures = queue.get()
        total_article_count += article_count
        parse_failures += local_parse_failures
        write_failures += local_write_failures
        process.join()


def list_html_files():
    try:
        files = os.listdir(repo_path)
        html_files = [f for f in files if f.endswith('.html')]
        return html_files
    except Exception as e:
        perror('Cannot list files in directory: %s' % (repo_path))
        traceback.print_exc()
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
repo_path = './repository/'  # Where downloaded HTML files are stored
corpus_path = './corpus/'  # Where corpus (parsed) text files will be stored
corpus_doc_suffix = '.txt'
corpus_doc_suffix_xml = '.xml'
parse_failures = []  # filenames of HTML files that text wasn't extracted
write_failures = []  # filenames of TXT files that couldn't be stored to disk
field_separator = '\n\n'
num_processors = os.cpu_count()
num_processes = num_processors # Number of processes used during preprocessing
total_article_count = 0  # How many articles where preprocessed by all processes


if __name__ == '__main__':
    main()

