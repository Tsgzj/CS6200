#!/usr/bin/env python

import os
import json
import unittest
from os import path

data_path = "/Users/Sun/Documents/IR/Data/HW1/ap89_collection/"

# Preprocess to get only DocNo and TEXT
## first get all file names
## return a list of all files
def list_all_files(file_dir):
    return [f for f in os.listdir(file_dir) if f.startswith("ap")]
    ## to ignore OS X system file .DS_store and readme

## parse files
## extract each document
def extract_docs(filename):
    docs = []

    for line in open(filename):
        l = line.strip()
        if l == "<DOC>":
            doc = {"docno": None, "text": ""}
            is_context = False

        elif l == "</DOC>":
            docs.append(doc)

        elif l.startswith("<DOCNO>") and l.endswith("</DOCNO>"):
            doc_no = l[8:-9]
            ## format of docno: <DOCNO> AP890101-0001 </DOCNO>
            doc["docno"] = doc_no

        elif line.startswith("<TEXT>"):
            is_context = True

        elif line.startswith("</TEXT>"):
            is_context = False

        elif is_context:
            doc["text"] += line

    return docs

# unit test
class TddIndex(unittest.TestCase):

    def test_get_full_list_of_files(self):
        result = list_all_files(data_path)
        self.assertEqual(len(result), 364) # in total 364 files

    def test_get_full_list_of_docs(self):
        all_docs = []
        for f in list_all_files(data_path):
            all_docs += extract_docs(data_path+f)
        self.assertEqual(len(all_docs), 84678) # number from doclist.txt

if __name__ == '__main__':
    unittest.main()
