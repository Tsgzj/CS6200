from index import parse_index
import operator

def read_res(res):
    index = parse_index("/Users/Sun/Documents/IR/Data/hw7/trec/full/index")

    count = 0
    hit = 0
    spam = {}

    spam_count = 0
    spam_hit = 0
    for line in open(res):
        docid, label, prob = line.split()

        label = float(label)
        prob = float(prob)

        if spam_count < 50:
            if docid not in index:
                spam_hit += 1
            spam_count += 1

        count += 1
        if label == 1 and docid not in index:
            hit += 1
        elif label == 0 and docid in index:
            hit += 1

    print res
    print "All test set: {} out of {}, accuracy {}".format(hit, count, float(hit)/float(count))
    print "Top 50 spams: {} out of {}, accuracy {}".format(spam_hit, "50", float(spam_hit)/50.0)
    print "\n"

read_res("./../data/mannual.sorted.txt")
read_res("./../data/all_unigram.sorted.txt")
