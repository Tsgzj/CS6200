from collections import defaultdict, OrderedDict

def read_qrel(infiles):
    """function to read qrel file"""

    qrel = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))

    for infile in infiles:
        for line in open(infile):
            try:
                qid, user, docid, rel = line.split()
            except:
                continue

            rel = int(rel)
            qrel[qid][docid][user] =  rel

    return qrel

def write(outfile, qrel):
    of = open(outfile, 'w')
    for qid in qrel:
        for docid in qrel[qid]:
            if len(qrel[qid][docid]) < 3:
                print qid
                print docid
                print qrel[qid][docid]
            for user in qrel[qid][docid]:
                of.write(qid + ' ' + user + ' ' + docid + ' ' + str(qrel[qid][docid][user]) + '\n')

qrel = read_qrel(['/Users/Sun/dropbox/CS6200_W_Sun/hw5/data/qrel_pankaj.txt',
                  '/Users/Sun/dropbox/CS6200_W_Sun/hw5/data/qrel_tianlu.txt',
                  '/Users/Sun/dropbox/CS6200_W_Sun/hw5/data/qrel_wen.txt'])
write('/Users/Sun/dropbox/CS6200_W_Sun/hw5/data/qrel_all.txt', qrel)
