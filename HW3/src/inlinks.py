from elasticsearch import Elasticsearch
from sets import Set

es = Elasticsearch()
inlink_map = {}
INDEX = 'test'
ids = {}
idsr = {}

def get_all_ids():
    res = es.search(
        index = INDEX,
        doc_type = "document",
        body = {
            'query' : {
                'match_all' : {}
            },
            'fields': ['docno', 'docno:'],
            'size': 100000
        },
        request_timeout = 60
    )
    for doc in res["hits"]["hits"]:
        try:
            ids[doc['_id']] = doc['fields']['docno'][0]
            idsr[doc['fields']['docno'][0]] = doc['_id']
        except:
            ids[doc['_id']] = doc['fields']['docno:'][0]
            idsr[doc['fields']['docno:'][0]] = doc['_id']

    print "All ids get"

def init_url(all_ids):
    for url in all_ids:
        inlink_map[all_ids[url]] = Set()
    print "Initialzing"

def get_outlinks_by_url(url):
    try:
        res = es.get(
            index = INDEX,
            doc_type = 'document',
            id = url
        )
    except:
        print "Cannot get outlinks"
        return []

    return res['_source']['out_links']#list of outlinks

def add_inlinks(all_ids):
    count = 0
    for item in all_ids:
        count += 1
        if count % 2000 == 0:
            print str(count)
        outlinks = get_outlinks_by_url(item) #base64 encoded url
        if outlinks:
            for docno in outlinks:
                if docno in inlink_map:
                    inlink_map[docno].add(ids[item])
    print "Inlink map built"

def update_url_inlinks(docno):
    try:
        es.update(
            index = INDEX,
            doc_type = 'document',
            id = idsr[docno],
            body = {
                'doc' : {
                    'in_links' : list(inlink_map[docno])
                }
            }
        )
    except:
        print "Update failed on " + docno
        return


def update_es(inlink_map):
    count = 0
    for item in inlink_map:
        count += 1
        if count % 2000 == 0:
            print str(count)
        update_url_inlinks(item)

if __name__ == "__main__":
    get_all_ids()
    init_url(ids)
    add_inlinks(ids)
    update_es(inlink_map)
