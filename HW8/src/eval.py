from collections import defaultdict, OrderedDict
import itertools

def read_qres(infile):
    temp_qrel = defaultdict(lambda: defaultdict(lambda: []))
    qrel = defaultdict(lambda: defaultdict(lambda: 0))
    num_qrel = defaultdict(lambda: 0)

    for line in open(infile):
        try:
            qid, _, docid, rel = line.split()
        except:
            continue

        rel = int(rel)
        qrel[qid][docid] = rel

    return qrel

def read_trec(infile):
    """function to read trec file"""
    trec = defaultdict(lambda: {})

    for line in open(infile):
        qid, _, docid, _, score, _ = line.split()
        if len(trec[qid]) < 1000:
            trec[qid][docid] = float(score)

    trec = OrderedDict(sorted(trec.items()))
    return trec

def get_doc_query_pair(qrel, trec):
    query_pair = []

    for qid in qrel:
        if qid in trec:
            for docid in qrel[qid]:
                if qrel[qid][docid] == 1:
                    query_pair.append((docid, qid))

    return query_pair

def read_cluster():
    clustering = {}
    for line in open("./../data/clustering.txt"):
        docid, cluster, topic = line.split()
        clustering[docid] = cluster

    return clustering

if __name__ == "__main__":
    trec = read_trec("./../../HW1/result/okapi_bm25.txt")
    qres = read_qres("/Users/Sun/Documents/IR/Data/HW1/qrels.adhoc.51-100.AP89.txt")

    query_pair = get_doc_query_pair(qres, trec)
    cluster = read_cluster()

    hit = 0
    count = 0
    for comb in list(itertools.combinations(query_pair, 2)):
        doc1 = comb[0][0]
        doc2 = comb[1][0]
        qid1 = comb[0][1]
        qid2 = comb[1][1]

        clu1 = cluster.get(doc1)
        clu2 = cluster.get(doc2)


        if clu1 and clu2:
            count += 1
            if qid1 == qid2 and clu1 == clu2:
                hit += 1
            elif qid1 != qid2 and clu1 != clu2:
                hit += 1

    print hit, count, float(hit) / float(count)
