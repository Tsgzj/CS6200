from elasticsearch import Elasticsearch

es = Elasticsearch()

def query_all_ids():
    res = es.search(
        index = "hw7",
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

def es_get_term_vector(did):
    res = es.termvectors(
        index = 'hw7',
        doc_type = "document",
        id = did,
        body = {
            'term_statistics' : True
        }
    )

    if res["term_vectors"].get("text"):
        lenth = len(res["term_vectors"]["text"]["terms"])

        if lenth < 10:
            print lenth, did

        return True
    else:
        return False

all_ids = query_all_ids()

count = 0
for iid in all_ids:
    if not es_get_term_vector(iid):
        print iid
        es.delete(
            index = "hw7",
            doc_type = "document",
            id = iid
        )
        count += 1

print count
