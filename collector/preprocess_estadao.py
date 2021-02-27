from lxml import etree
from os import listdir
import os
import time
import locale
from datetime import datetime
import json
import argparse
import re
from dateutil import parser as tparser

# python3 htmltotext.py -d news/ -o data.json -l log.txt

WRITING_BATCH_SIZE = 1024
PASS_LIST = ["___esportefera.com.br", "br_galerias_", "br_fotos", ".DS_Store", "emais.estadao.com.br", "jornaldocarro.estadao.com.br", "___tv.estadao"]
locale.setlocale(locale.LC_TIME, 'pt_BR')

def url_to_filename(url):
    return url.replace('://', '___').replace('/', '_')

parser = argparse.ArgumentParser(
    description='Extract text and other information from html files for Estadão journal')
parser.add_argument('-d', '--html_files_dir', help='path to the directory of html files',
                    required=True)
parser.add_argument('-o', '--output_file', help='file to write json output data',
                    default='estadao_extracted.json')
parser.add_argument('-l', '--log_file', help='file to log names of the files couldn\'t be processed',
                    default='estadao_extraction_log.txt')

args = vars(parser.parse_args())

if __name__ == '__main__':

    unprocessed_files = []
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

                    htmltext = open(os.path.join(html_files_dir, filename), "r").read()
                    tree = etree.HTML(re.sub('(<!--.*?-->)', ' ', htmltext), htmlparser)
                    title, text = "", ""
                    # extract info from html file
                    if "noticia__state" in htmltext:
                        text = ""
                        if 'class="n--noticia__subtitle"' in htmltext:
                            text += ' '.join(tree.xpath('//*[@class="n--noticia__subtitle"]//text()')).replace('\n', '')
                        if 'class="n--noticia__content content"' in htmltext:
                            text += "\n" + ' '.join(tree.xpath('//*[@class="n--noticia__content content"]/p/text()')).replace('\n', '')
                            if text == "\n":
                                text += ' '.join(tree.xpath('//*[@class="n--noticia__content content"]/p/span/text()')).replace('\n', '')
                        
                        title = ' '.join(tree.xpath('//*[@class="n--noticia__title"]//text()')).replace('\n', '')
                        if "br18." in htmltext:
                            date = ''.join(tree.xpath('//*[@id="data"]//text()')).replace(' ', '').replace('\n', '')
                            date = datetime.strptime(date, "%d.%m.%Y|%Hh%M")
                        else:
                            date = ''.join(tree.xpath('//*[@class="n--noticia__state"]/p/text()')).replace(' ', '').replace('\n', '')
                            if "Atualizado" in date:
                                date = date[:date.index("Atualizado")]
                            date = datetime.strptime(date, "%d%B%Y|%Hh%M")
                    elif "h1 class=\"title\"" in htmltext:
                        text = ' '.join(tree.xpath('//*[@class="content-text content"]/p/text()')).replace('\n', '')
                        date = ' '.join(tree.xpath('//p[@class="data-post"]//text()')).replace('\n', '').replace(" ", "")
                        if date != "":
                            date = datetime.strptime(date, "%d/%m/%Y|%Hh%M")
                        else:
                            date = ' '.join(tree.xpath('//span[@class="data-post"]//text()')).replace('\n', '').replace(" ", "")
                            date = datetime.strptime(date, "%d%B%Y|%H:%M")
                        title = ' '.join(tree.xpath('//*[@class="thumb-tit"]//text()')).replace('\n', '')
                    else:
                        unprocessed_files.append(filename)
                        print("Unknown format:", filename)
                        print(filename, file=log_file)

                    url = filename.replace("___", "://")
                    url = url.replace("_", "/")

                    # 
                    text = re.sub(r"Veja também: '[^']*'", "\n", text)

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
    print('Processed', pro_files_no, 'files in {0:.2f}'.format(elapsed_time),
        'sec, approximately {0:.2f}'.format(pro_files_no/elapsed_time), 'file per sec')
    print(len(unprocessed_files), 'files could not be processed')
    print(len(passed_files), 'files was passed')