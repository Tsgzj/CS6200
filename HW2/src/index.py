#!/usr/bin/env python
from collections import deque
from tokenizer import init_stopwords, analyze_term, tokenize
import json
import re
from b64 import d_b64

INDEX_DIR =  "/Users/Sun/Documents/IR/Data/HW2/index/"

class Index:
    def __init__(self):
        print "Index initing..."
        self.__read_map()
        self.__read_cat()
        self.__read_idmap()
        self.__index = open(INDEX_DIR + 'index')
        self.__stopwords = init_stopwords()
        self.__term_size = len(self.__catalog)
        self.__pat = re.compile(r'([-_a-zA-Z0-9]+)')
        print str(len(self.__doc_map)) + " docs indexed"

    def __read_cat(self):
        self.__catalog = {}
        for i in open(INDEX_DIR + 'catalog'):
            a = i.split(' ')
            self.__catalog[a[0]] = int(a[1])

    def __read_map(self):
        self.__doc_map = {}
        for i in open(INDEX_DIR + 'map'):
            a = i.split(' ')
            self.__doc_map[a[0]] = int(a[1])

    def __read_idmap(self):
        self.__id_map = {}
        for i in open(INDEX_DIR + 'idmap'):
            a = i.split(' ')
            self.__id_map[a[0]] = a[1]

    def doc_map(self):
        return self.__doc_map

    def catalog(self):
        return self.__catalog

    def search(self, term):
        t = analyze_term(term, self.__stopwords)
        if t in self.__catalog:
            self.__index.seek(self.__catalog[t])
            l = self.__index.readline().split(' ')
            de_res = json.loads(self.__pat.sub(r'"\1"', l[1]))
            res = {}
            for item in de_res:
                d_ttf = []
                for pos in de_res[item]:
                    d_ttf.append(int(d_b64(pos)))
                res[item] = d_ttf
            return res
        else:
            return {}

    def analyze(self, string):
        return tokenize(string, self.__stopwords)

    def length_of_doc(self, docid):
        if docid in self.__doc_map:
            return self.__doc_map[docid]
        else:
            return 0

    def term_size(self):
        return self.__term_size

    def get_doc_name(self, docid):
        return self.__id_map[docid]

# i = Index()
# print len(i.search('partyman'))
# print len(i.search('imported'))
# print len(i.search('waterway'))
