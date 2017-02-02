from __future__ import division
import math
import sys
from collections import defaultdict, OrderedDict
import matplotlib.pyplot as plt

CUTOFFS = (5, 10, 20, 50, 100)

def read_qrel(infile):
    """function to read qrel file"""

    temp_qrel = defaultdict(lambda: defaultdict(lambda: []))
    qrel = defaultdict(lambda: defaultdict(lambda: 0))
    num_qrel = defaultdict(lambda: 0)

    for line in open(infile):
        try:
            qid, _, docid, rel = line.split()
        except:
            continue

        rel = int(rel)
        temp_qrel[qid][docid].append(rel)

    for qid in temp_qrel:
        for docid in temp_qrel[qid]:
            rels = temp_qrel[qid][docid]
            total_rel = 0
            for item in rels:
                total_rel += item
            qrel[qid][docid] = total_rel / len(rels)
            if math.floor(total_rel / len(rels)) > 0:
                num_qrel[qid] += 1
    return qrel, num_qrel

def read_trec(infile):
    """function to read trec file"""
    trec = defaultdict(lambda: {})

    for line in open(infile):
        qid, _, docid, _, score, _ = line.split()
        if len(trec[qid]) < 1000:
            trec[qid][docid] = float(score)

    trec = OrderedDict(sorted(trec.items()))
    return trec

def calculate(qrel, num_qrel, trec, each_query):
    """function to calculate scores"""
    total_ret = 0
    total_rel = 0
    total_rel_ret = 0
    sum_rec_at_cutoffs = [0] * len(CUTOFFS)
    sum_prec_at_cutoffs = [0] * len(CUTOFFS)
    sum_f1_at_cutoffs = [0] * len(CUTOFFS)
    sum_avg_prec = 0
    sum_r_prec = 0
    sum_ndcg = 0

    for topic in trec:
        trecmap = sorted(trec[topic].iteritems(), key=lambda i: i[1], reverse=True)

        len_list = len(trecmap) + 1
        num_ret = 0
        num_rel_ret = 0
        prec_list = [None] * len_list
        rec_list = [None] * len_list
        rels = [None] * len_list
        sum_pres = 0
        num_all_rel = num_qrel[topic]

        for docid, score in trecmap:
            rels[num_ret] = qrel[topic][docid]
            num_ret += 1
            if math.floor(qrel[topic][docid]) > 0:
                sum_pres += (1 + num_rel_ret) / num_ret
                num_rel_ret += 1

            prec_list[num_ret] = num_rel_ret / num_ret
            rec_list[num_ret] = num_rel_ret / num_all_rel

        avg_prec = sum_pres / num_all_rel
        final_rec = num_rel_ret / num_all_rel

        for i in xrange(num_ret + 1, 1001):
            rec_list[i] = final_rec
            prec_list[i] = num_rel_ret / i

        # calculate cutoffs
        rec_at_cutoffs = []
        prec_at_cutoffs = []
        f1_at_cutoffs = []
        for cutoff in CUTOFFS:
            prec = prec_list[cutoff]
            rec = rec_list[cutoff]
            prec_at_cutoffs.append(prec)
            rec_at_cutoffs.append(rec)
            f1 = 0
            if prec > 0 and rec > 0:
                f1 = 2 * prec * rec / (prec + rec)
            f1_at_cutoffs.append(f1)

        # calculate R prec
        if num_all_rel > num_ret:
            rp = num_rel_ret / num_ret
        else:
            rp = prec_list[num_all_rel]

        # calculate ndcg
        idcg = dcg(sorted(rels, reverse = True), num_ret)
        if idcg > 0:
            ndcg = dcg(rels, num_ret) / dcg(sorted(rels, reverse = True), num_ret)
        else:
            ndcg = 0

        if each_query:
            print_result(topic, num_ret, num_all_rel, num_rel_ret, avg_prec,
                         rec_at_cutoffs, prec_at_cutoffs, rp, f1_at_cutoffs, ndcg)

        prec_max = 0
        max_prec_list = [0] * 1001
        for i in xrange(1000, 0, -1):
            prec_max = max(prec_max, prec_list[i])
            max_prec_list[i] = prec_max

        # plot_result(topic, max_prec_list, prec_list, rec_list)

        total_ret += num_ret
        total_rel += num_all_rel
        total_rel_ret += num_rel_ret
        for i in xrange(len(CUTOFFS)):
            sum_rec_at_cutoffs[i] += rec_at_cutoffs[i]
            sum_prec_at_cutoffs[i] += prec_at_cutoffs[i]
            sum_f1_at_cutoffs[i] += f1_at_cutoffs[i]

        sum_avg_prec += avg_prec
        sum_r_prec += rp
        sum_ndcg += ndcg

    query_num = len(trec)
    avg_rec_at_cutoffs = []
    avg_prec_at_cutoffs = []
    avg_f1_at_cutoffs = []
    for i in sum_rec_at_cutoffs:
        avg_rec_at_cutoffs.append(i / query_num)
    for i in sum_prec_at_cutoffs:
        avg_prec_at_cutoffs.append(i / query_num)
    for i in sum_f1_at_cutoffs:
        avg_f1_at_cutoffs.append(i / query_num)
    avg_avg_prec = sum_avg_prec / query_num
    avg_rp = sum_r_prec / query_num
    avg_ndcg = sum_ndcg / query_num

    print_result(query_num, total_ret, total_rel, total_rel_ret, avg_avg_prec,
                 avg_rec_at_cutoffs, avg_prec_at_cutoffs, avg_rp, avg_f1_at_cutoffs, avg_ndcg)

def dcg(rels, p):
    res = rels[0]
    for i in xrange(1, p):
        res += rels[i] / math.log(i + 1, 2)
    return res

def plot_result(qid, max_plist, plist, rlist):
    axes = plt.gca()
    axes.set_xlim([0, 1])
    axes.set_ylim([0, 1])
    plt.plot(rlist, plist)
    max_plist.append(0)
    max_plist[0] = 1.0
    rlist.append(1.0)
    rlist[0] = 0.0
    plt.plot(rlist, max_plist)
    plt.ylabel("Precisions")
    plt.xlabel("Recalls")
    plt.title(qid)
    plt.savefig(qid + ".png")
    plt.close()

def print_result(qid, num_ret, num_rel, num_rel_ret, avg_avg_prec, avg_rec_at_cutoffs,
                 avg_prec_at_cutoffs, avg_r_prec, f1_at_cutoffs, ndcg):
    print "\n"
    print "Queryid (Num):  {:>6}".format(qid)
    print "Total number of documents"
    print "  Retrieved:    {:6d}".format(num_ret)
    print "  Relevant:     {:6d}".format(num_rel)
    print "  Rel_ret:      {:6d}".format(num_rel_ret)
    print "R-Precision:"
    print "                {:5.4f}".format(avg_r_prec)
    print "Average precision:"
    print "                {:5.4f}".format(avg_avg_prec)
    print "nDCG:"
    print "                {:5.4f}".format(ndcg)
    print "--------------------------------------------------------------"
    print "    k           Precision            Recall           F1"
    for i in range(len(CUTOFFS)):
        print "  {:3}           {:5.4f}               {:5.4f}           {:5.4f}".format(CUTOFFS[i],
                                                                                     avg_prec_at_cutoffs[i],
                                                                                     avg_rec_at_cutoffs[i],
                                                                                     f1_at_cutoffs[i])
if __name__ == "__main__":
    if len(sys.argv) == 4:
        if sys.argv[1] == "-q":
            fqrel = sys.argv[2]
            ftrec = sys.argv[3]
            qrel, num_qrel = read_qrel(fqrel)
            trec = read_trec(ftrec)
            calculate(qrel, num_qrel, trec, True)
        else:
            print "Usage:  trec_eval [-q] <qrel_file> <trec_file>"
    elif len(sys.argv) == 3:
        fqrel = sys.argv[1]
        ftrec = sys.argv[2]
        qrel, num_qrel = read_qrel(fqrel)
        trec = read_trec(ftrec)
        calculate(qrel, num_qrel, trec, False)
    else:
        print "Usage:  trec_eval [-q] <qrel_file> <trec_file>"
