from lxml import etree
from os import listdir
import os
import time
from datetime import datetime
import json
import argparse
import re
from dateutil import parser as tparser

# python3 htmltotext.py -d news/ -o data.json -l log.txt

reg = re.compile("location.replace\(\s*\"http\:\/\/www1\.folha\.uol\.com\.br\/.*\"\s*\)\s*;")  # matching
rep = re.compile("location.replace\(\s*\"|\"\s*\)\s*;") # replace

WRITING_BATCH_SIZE = 1024
PASS_LIST = ['indicadores', ] 

def url_to_filename(url):
    return url.replace('://', '___').replace('/', '_')

parser = argparse.ArgumentParser(
    description='Extract text and other information from html files for Folha journal')
parser.add_argument('-d', '--html_files_dir', help='path to the directory of html files',
                    required=True)
parser.add_argument('-o', '--output_file', help='file to write json output data',
                    default='folha_extracted.json')
parser.add_argument('-r', '--redirected_file', help='file to write redirected files list',
                    default='redirected.csv')
parser.add_argument('-l', '--log_file', help='file to log names of the files couldn\'t be processed',
                    default='folha_extraction_log.txt')

args = vars(parser.parse_args())

if __name__ == '__main__':

    unprocessed_files = []
    redirected_files = []
    passed_files = []
    pro_files_no = 0
    htmlparser = etree.HTMLParser()
    starttime = time.time()
    html_files_dir = args['html_files_dir']

    writing_chunk = ''
    with open(args['log_file'], 'w') as log_file:
        with open(args['output_file'], 'w') as fo:
            for filename in listdir(html_files_dir):
                try:
                    # open the file
                    if any(x in filename for x in PASS_LIST):
                        passed_files.append(filename)
                        continue

                    htmltext = open(os.path.join(html_files_dir, filename), "r", encoding="ISO-8859-1").read()
                    tree = etree.HTML(re.sub('(<!--.*?-->)', ' ', htmltext), htmlparser)
                    
                    # files that have link redirection
                    redirection_links = re.findall(reg, htmltext)
                    if len(redirection_links) != 0:
                        redirected_files.append((filename, url_to_filename(re.sub(rep, "", redirection_links[0]))))
                        continue

                    # extract info from html file
                    elif 'id="articleNew"' in htmltext:
                        text = ' '.join(tree.xpath('//*[@id="articleNew"]/p//text()')).replace('\n', '')
                        title = ' '.join(tree.xpath('//*[@id="articleNew"]/h1//text()')).replace('\n', '')
                        date = ''.join(tree.xpath('//div[@id="articleDate"]//text()'))
                        date = tparser.parse(re.sub(r"\s", "", date))

                    elif 'class="article"' in htmltext:
                        text = ' '.join(tree.xpath('//*[@class="article"]')[0].itertext()).replace('\n', '') 
                        title = ' '.join(tree.xpath('//*[@class="article"]/h1//text()')).replace('\n', '')
                        date = ''.join(tree.xpath('//span[@class="data"]/text()'))
                        date = tparser.parse(re.sub(r"\s", "", date))
                    
                    elif 'itemprop="articleBody"' in htmltext:
                        text = ' '.join(tree.xpath('//*[@itemprop="articleBody"]/p//text()')).replace('\n', '')
                        title = ' '.join(tree.xpath('//*[@itemprop="headline"]/text()')).replace('\n', '')
                        date = tree.xpath('//time')[-1].attrib
                        date = tparser.parse(date['datetime'])

                    elif 'name="conteudo"' in htmltext:
                        text = ' '.join(tree.xpath('//article/div/p//text()')).replace('\n', '')
                        title = ' '.join(tree.xpath('//article/header/h1//text()')).replace('\n', '')
                        date = (tree.xpath('//meta[@property="article:published_time"]') + tree.xpath('//meta[@property="og:published_time"]')) [-1].attrib
                        date = tparser.parse(date['content'])                    
                    
                    else:
                        print('No match :', filename)
                        unprocessed_files.append(filename)
                        print(filename, file=log_file)
                        continue

                    if len(title) == 0:
                        title = "none"

                    url = filename.replace("___", "://")
                    url = url.replace("_", "/")

                    # write info to json file
                    writing_chunk += json.dumps({
                        'date_text': datetime.strftime(date, "%d/%m/%Y %H:%M"),
                        'title': title,
                        'file': filename,
                        'url': url,
                        'text': text
                        }, ensure_ascii=False) + '\n'
                    
                    pro_files_no += 1

                    if pro_files_no % WRITING_BATCH_SIZE == 0:
                        fo.write(writing_chunk)
                        writing_chunk = ''

                except Exception as e:
                    unprocessed_files.append(filename)
                    print("Error while parsing file:", filename, e)
                    print(filename, file=log_file)

            if writing_chunk != '':
                fo.write(writing_chunk)

    elapsed_time = time.time() - starttime
    with open(args['redirected_file'], 'w') as fo:
        import csv
        csv_out = csv.writer(fo)
        for row in redirected_files:
            csv_out.writerow(row)

    print('Processed', pro_files_no, 'files in {0:.2f}'.format(elapsed_time),
        'sec, approximately {0:.2f}'.format(pro_files_no/elapsed_time), 'file per sec')
    print(len(unprocessed_files), 'files could not be processed')
    print(len(passed_files), 'files was passed')
    print(len(redirected_files), 'files have redirection links')
    

