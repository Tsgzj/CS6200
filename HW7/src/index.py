import json
import os
import email
import re
import codecs
from bs4 import BeautifulSoup
from langdetect import detect
from os import path
from multiprocessing import Pool
from elasticsearch import Elasticsearch
from stemming.porter import stem

es = Elasticsearch()
Data_path = "/Users/Sun/Documents/IR/Data/HW7/trec/"
dic = set()

def list_all_files_start_with(file_dir, start):
    return [f for f in os.listdir(file_dir) if f.startswith(start)]
    ## to ignore OS X system file .DS_store and readme

# def parse_html_mail(hfile):

def parse_index(infile):
    index = set()
    for line in open(infile):
        try:
            label, ab_name = line.split()
        except:
            continue

        name = path.basename(ab_name)
        if label == "ham":
            index.add(name)
    return index

def parse_multi_payload(p):
    result = []
    if p.is_multipart():
        for payload in p.get_payload():
            result += parse_multi_payload(payload)
    else:
        result.append(p.get_payload())

    return result

def parse_mail(emfile):
    b = email.message_from_file(open(Data_path + "data/" + emfile))

    body = []
    if b.is_multipart():
        body = parse_multi_payload(b)
    else:
        ctype = b.get_content_type()
        if ctype == 'image/jpeg':
            return ''
        body.append(b.get_payload())

    all_body = ''.join(body)
    # if b.is_multipart():
    #     for part in b.walk():
    #         ctype = part.get_content_type()
    #         cdispo = str(part.get('Content-Disposition'))

    #         # skip any text/plain (txt) attachments
    #         if ctype == 'text/plain' and 'attachment' not in cdispo:
    #             body = part.get_payload(decode=True)  # decode
    #             break
    # # not multipart - i.e. plain text, no attachments, keeping fingers crossed
    # else:
    #     body = b.get_payload()


    try:
        soup = BeautifulSoup(all_body, "lxml")
    except:
        return all_body

    text = soup.findAll(text = True)
    return ''.join(text)

def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match(r'<!--.*-->', element):
        return False
    return True

def init_dic():
    dic = set()
    for line in open("/Users/Sun/Documents/IR/Data/HW7/dictionary.txt"):
        try:
            word = line.strip()
        except:
            continue

        dic.add(word.lower())

    return dic

def clean(text):
    # if len(text) < 1:
    #     return None

    # word_list = text.lower().split()
    # cleaned = []
    # for word in word_list:
    #     if word in dic:
    #         cleaned.append(word)

    # return ' '.join(x for x in cleaned)

    global dic
    cleaned = []

    if len(text) < 1:
        return ''

    for word in text.lower().split():
        if len(word) < 18:
            dic.add(word)
            cleaned.append(word)
        else:
            try:
                num = int(word)
            except:
                try:
                    num = float(word)
                except:
                    continue

            cleaned.append(str(num))
            dic.add(str(num))

    return ' '.join(x for x in cleaned)

    # try:
    #     lang = detect(text)
    # except:
    #     return None

    # if lang == 'en':
    #     for word in text.lower().split():
    #         dic.add(word)
    #     return text
    # else:
    #     return None

def wrapper(docno, text):
    global index
    if docno in index:
        label = "ham"
    else:
        label = "spam"

    return {
        'text': text,
        'docno': docno,
        'label': label
    }

def add_to_index(docno, text):
    if len(text) > 1:
        es.index(
            index = "hw7",
            doc_type = "document",
            id = docno,
            body = wrapper(docno, text)
        )

def process(em):
    global dic
    text = clean(parse_mail(em))
    add_to_index(em, text)

if __name__ == "__main__":
    # dic = init_dic()
    index = parse_index(Data_path + "full/index")
    all_inmail = list_all_files_start_with(Data_path + "data/", "inmail")
    # p = Pool(20)
    # p.map(process, all_inmail)
    for i in all_inmail:
        process(i)

    of = codecs.open("./../data/all_dic.txt", "w", "utf-8")
    print len(dic)
    for i in dic:
        of.write(i + "\n")
