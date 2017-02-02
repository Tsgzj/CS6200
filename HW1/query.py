#!/usr/bin/env python
from elasticsearch import Elasticsearch
from nltk.corpus import stopwords

INDEX = "ap_dataset"
QUERY_FILE = "/Users/Sun/Documents/IR/Data/HW1/query.modified.txt"

# Use stoplist to filter query before actually search elastic
#word_list = open("/Users/Sun/Documents/IR/Data/HW1/stoplist.txt", "r")
#stop_words = set()
#for line in word_list:
#    for w in line.split():
#        if w.lower() not in stop_words:
#            stop_words.add(w.lower())
#            # load stoplist.txt
## No need to filter query, use elasticsearch analyze instead

def query_term(client, term):
    # return a list of ids and corresponding score
    res = client.search(
        index = INDEX,
        doc_type = 'document',
        body = {
            'query': {
                'function_score': {
                    'query': {
                        'match': {
                            'text': term
                        }
                    },
                    'functions': [
                        {
                            'script_score': {
                                'script_id': 'getTF',
                                'lang' : 'groovy',
                                'params': {
                                    'term': term,
                                    'field': 'text'
                                }
                            }
                        }
                    ],
                    'boost_mode': 'replace'
                }
            },
            'size': 100000,
            'fields': ['']
        }
    )
    matrix = []
    for i in res["hits"]["hits"]:
        doc = {}
        doc["id"] = i["_id"]
        doc["score"] = i["_score"]
        matrix.append(doc)

    return matrix

def terms_of_query(client, query_string):
    tokens = query_string.split('.', 2)
    terms = {"id": tokens[0], "terms": analyze_string(client, tokens[1])}
    return terms

def query_term_size(client):
    return client.search(
        index = INDEX,
        body = {
            'aggs':{
                'unique_terms': {
                    'cardinality': {
                        'script': 'doc["text"].values'
                    }
                }
            }
        }
    )["aggregations"]["unique_terms"]["value"]

def query_all_ids(client):
    res = client.search(
        index = INDEX,
        doc_type = "document",
        body = {
            'query' : {
                'match_all' : {}
            },
            'fields': [],
            'size': 100000
        }
    )
    ids = []
    for doc in res["hits"]["hits"]:
        ids.append(doc["_id"])

    return ids

def length_of_doc(client, doc_id):
    res = client.termvectors(
        index = INDEX,
        doc_type = "document",
        id = doc_id
    )
    total = 0
    for t in res["term_vectors"]["text"]["terms"]:
        value = res["term_vectors"]["text"]["terms"].get(t)
        total += value["term_freq"]
    return total

def query_vector(client, doc_id):
    resp = client.termvectors(
        index = INDEX,
        doc_type = "document",
        id = doc_id
    )
    res = {"id": doc_id, "tf": {}}
    tf = {}
    if "text" in resp["term_vectors"]:
        for t in resp["term_vectors"]["text"]["terms"]:
            tf[t] = resp["term_vectors"]["text"]["terms"].get(t)["term_freq"]
            res["tf"] = tf
    return res

# def filter_query(query):
#     # return terms that are not stop words in query
#     return [word for word in query.lower().split() if word not in stop_words]
def analyze_string(client, string):
    res = client.indices.analyze(
        index = INDEX,
        analyzer = "my_english",
        text = string
    )
    tokens = set()
    for t in res["tokens"]:
        tokens.add(t["token"])

    return tokens

#es = Elasticsearch()
#print query_term_size(es)
#print len(query_term(es, 'algorithm'))
#print len(query_term(es, 'cat'))
#for t in analyze_string(es, "this is a string to be analyzed"):
#    query_term(es, t)
#length_of_doc(es, "AP890111-0252")
