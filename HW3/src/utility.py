from elasticsearch import Elasticsearch
from sets import Set

es = Elasticsearch(timeout= 90)

res = es.search(
    index='test',
    doc_type='document',
    body={
        'query':{
            'match_all':{}
        },
        'fields':['docno'],
        'size': 100000
    })

print "All ids"

mapping = {}
myset = Set()
for doc in res['hits']['hits']:
    if doc['fields']['docno'][0] not in mapping:
        mapping[doc['fields']['docno'][0].lower()] = doc['_id']
        myset.add(doc['fields']['docno'][0].lower())
    else:
        print doc['_id']
        print doc['fields']['docno']

print len(myset)
