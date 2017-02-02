#!/usr/bin/env python
from index import Index

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
    matrix = []
    res = client.search(term)
    for i in res:
        doc = {}
        doc["id"] = i
        doc["pos"] = res[i]
        matrix.append(doc)

    return matrix

def terms_of_query(client, query_string):
    tokens = query_string.split('.', 2)
    terms = {"id": tokens[0], "terms": analyze_string(client, tokens[1])}
    return terms

def query_term_size(client):
    return client.term_size()

def query_all_ids(client):
    return client.doc_map().keys()

def length_of_doc(client, doc_id):
    return client.length_of_doc(doc_id)

# def filter_query(query):
#     # return terms that are not stop words in query
#     return [word for word in query.lower().split() if word not in stop_words]
def analyze_string(client, string):
    res = client.analyze(string)
    tokens = set()
    for t in res:
        tokens.add(t)
    return tokens

#es = Elasticsearch()
#print query_term_size(es)
#print len(query_term(es, 'algorithm'))
#print len(query_term(es, 'cat'))
#for t in analyze_string(es, "this is a string to be analyzed"):
#    query_term(es, t)
# i = Index()
# print query_term(i, '0.0')
# print terms_of_query(i, '42. a machine translation system.')
# print length_of_doc(i, "1110252")
# print len(query_all_ids(i))
# print query_term_size(i)
