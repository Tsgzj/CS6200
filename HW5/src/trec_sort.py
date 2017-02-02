from collections import defaultdict, OrderedDict
import operator

def read_trec(infile):
    """function to read trec file"""
    trec = defaultdict(lambda: {})
    of = open('/Users/Sun/Documents/Trec_HW5.sorted.txt', 'w')
    for line in open(infile):
        qid, _, docid, _, score, _ = line.split()
        trec[qid][docid] = float(score)

    for qid in trec:
        sorted_x = sorted(trec[qid].items(), key=operator.itemgetter(1), reverse = True)
        for item in sorted_x:
            of.write(qid + ' Q0 ' + item[0] + ' 0 ' + str(item[1]) + ' Exp\n')

read_trec("/Users/Sun/Documents/Trec_Text_HW5.txt")
