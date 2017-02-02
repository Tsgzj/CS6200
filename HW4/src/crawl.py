import operator
import base64
from page_rank import page_rank
from elasticsearch import Elasticsearch

es = Elasticsearch(timeout = 1000)

def query_all_ids():
    res = es.search(
        index = "test",
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

def query_by_id(did):
    res = es.get(
        index="test",
        doc_type="document",
        id=did
    )
    return res

def build_graph():
    graph = {}

    ids = query_all_ids()
    print "Get all ids"

    for i in ids:
        info = query_by_id(i)
        docid = base64.urlsafe_b64decode(info["_id"].encode('utf-8'))
        graph[docid] = {'inlinks': [], 'outlinks': 0}
        if info["_source"].get("in_links"):
            for ilk in info["_source"]["in_links"]:
                if ilk in graph:
                    graph[docid]["inlinks"].append(ilk)

    count = 0
    for item in graph:
        if count % 2000 == 0:
            print count
        for ilk in graph[item]['inlinks']:
            if graph.get(ilk):
                graph[ilk]['outlinks'] += 1
        count += 1

    print "Finish building graph"
    return graph

if __name__ == '__main__':
    wg = build_graph()
    rank = page_rank(wg)

    sorted_rank = sorted(rank.items(), key = operator.itemgetter(1), reverse = True)

    outf = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW4/data/crawl_pagerank.txt", "a")

    for i in range(500):
        res = sorted_rank[i][0] + " " + str(sorted_rank[i][1]) + "\n"
        outf.write(res)
