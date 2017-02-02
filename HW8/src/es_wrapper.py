from __future__ import division
import math
import elasticsearch

def es_get_term_vector(client, did):
    res = client.termvectors(
        index = 'ap_dataset',
        doc_type = "document",
        id = did,
        body = {
            'term_statistics' : True
        }
    )

    try:
        info = res["term_vectors"]["text"]["terms"]
        return info
    except:
        return None

def es_get_text(client, did):
    res = client.get(
        index = 'ap_dataset',
        doc_type = 'document',
        id = did
    )
    return res['_source']['text']

def es_get_all_ids(client):
    res = client.search(
        index = "ap_dataset",
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

def calculate_score(tf, num_doc, doc, l):
    idf = 1 + math.log(num_doc/(doc + 1))
    tf = math.sqrt(tf)
    norm = 1 / math.sqrt(l)

    return tf * idf * norm
