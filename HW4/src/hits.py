from __future__ import division
import base64
import operator
from math import sqrt
from collections import defaultdict
from elasticsearch import Elasticsearch

ROOT_FILE = "/Users/Sun/Documents/IR/Data/HW4/root_encoded.txt"

BASE = set()
ROOT = set()
BASE_INLINKS = defaultdict(list)
BASE_OUTLINKS = defaultdict(list)
INLINKS = defaultdict(list)
OUTLINKS = defaultdict(list)
Allurl = set()

es = Elasticsearch(timeout = 1000)
THRESHOLD = 0.0000001

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
    global Allurl
    global INLINKS
    global OUTLINKS

    ids = query_all_ids()
    print "Get all ids"

    count = 0
    for i in ids:
        if count % 10000 == 0:
            print count
        info = query_by_id(i)
        docid = base64.urlsafe_b64decode(info["_id"].encode('utf-8'))
        Allurl.add(docid)
        if info["_source"].get("in_links"):
            INLINKS[docid] = info["_source"]["in_links"]
        else:
            INLINKS[docid] = []
        if info["_source"].get("out_links"):
            OUTLINKS[docid] = info["_source"]["out_links"]
        else:
            OUTLINKS[docid] = []
        count += 1

    print "Finish building graph"

def decode_base_urls():
    infile = open(ROOT_FILE)
    outfile = open("/Users/Sun/Documents/IR/Data/HW4/root.txt", 'a')
    for line in infile:
        url = base64.urlsafe_b64decode(line.strip())
        outfile.write(url + "\n")

def create_root():
    res = es.search(
        index="test",
        doc_type="document",
        body= {
            'query' : {
                'match_phrase' : {
                    'text': {
                        'query': "harvard university"
                    }
                }
            },
            'size': 1000
        }
    )

    for item in res["hits"]["hits"]:
        ROOT.add(item["_source"]["docno"])

    outfile = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW4/root_url.txt", "a")
    for item in ROOT:
        outfile.write(item + "\n")

    print "Root set initialized, size " + str(len(ROOT))

def expand_base():
    for url in ROOT:
        BASE.add(url)

    expand = set()
    for url in BASE:
        for ilk in INLINKS[url][:50]:
            if ilk not in BASE:
                expand.add(ilk)
        for olk in OUTLINKS[url]:
            if olk not in BASE:
                expand.add(olk)

    for url in expand:
        BASE.add(url)

    print "Base set initialized, size " + str(len(BASE))

def HITS():
    auths = defaultdict(lambda: 1)
    hubs = defaultdict(lambda: 1)

    for url in BASE:
        auths[url] = 1
        hubs[url] = 1

    Converged = False
    loop = 1
    while not Converged:
        print "Loop No." + str(loop)
        nextAuths = defaultdict(lambda: 0)
        tempAuths = defaultdict(lambda: 0)
        nextHubs = defaultdict(lambda: 0)
        tempHubs = defaultdict(lambda: 0)
        norm = 0

        for url in ROOT:
            tempAuths[url] = 0
            for ilk in INLINKS[url]:
                tempAuths[url] += hubs[ilk]
            norm += tempAuths[url]**2
        norm = sqrt(norm)
        for url in tempAuths:
            nextAuths[url] = tempAuths[url] / norm

        norm = 0
        for url in BASE:
            tempHubs[url] = 0
            for olk in OUTLINKS[url]:
                tempHubs[url] += auths[olk]
            norm += tempHubs[url]**2
        norm = sqrt(norm)
        for url in tempHubs:
            nextHubs[url] = tempHubs[url] / norm

        Converged = converge(hubs, nextHubs) and converge(auths, nextAuths)
        hubs = nextHubs
        auths = nextAuths
        loop += 1

    write500(hubs, "HUB.txt")
    write500(auths, "AUTH.txt")

def write500(data, name):
    outfile = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW4/data/hits_" + name, 'a')
    sorted_rank = sorted(data.items(), key = operator.itemgetter(1), reverse = True)
    for i in range(500):
        res = sorted_rank[i][0] + '\t' + str(sorted_rank[i][1]) + '\n'
        outfile.write(res)

def converge(rank0, rank1):
    isConverge = True
    for item in rank0:
        isConverge = isConverge and (abs(rank0[item] - rank1[item]) < THRESHOLD)
    return isConverge

if __name__ == "__main__":
    # decode_base_urls()
    build_graph()
    create_root()
    expand_base()
    HITS()
