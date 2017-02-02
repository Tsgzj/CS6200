from collections import defaultdict, OrderedDict
import math

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

def generate_doc_list(trec, qres):
    doc_list = defaultdict(lambda: set())

    for qid in trec:
        for docid in trec[qid]:
            doc_list[qid].add(docid)

    for qid in qres:
        if qid in trec:
            for docid in qres[qid]:
                if qres[qid][docid] == 1:
                    doc_list[qid].add(docid)

    of = open("./../data/doc_list", "w")
    for qid in doc_list:
        for docid in doc_list[qid]:
            of.write(qid + " " + docid + "\n")

if __name__ == "__main__":
    trec = read_trec("./../../HW1/result/okapi_bm25.txt")
    qres = read_qres("/Users/Sun/Documents/IR/Data/HW1/qrels.adhoc.51-100.AP89.txt")

    generate_doc_list(trec, qres)
