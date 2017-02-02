from collections import defaultdict
from sklearn import linear_model
from numpy import array
import operator
import random

qids = [85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94, 100, 89, 61, 94, 68, 57, 97, 98, 60, 80, 63, 91]
models = {"okapi_bm25", "okapi_tf", "tf_idf", "unigram_jm", "unigram_lap"}


labels = defaultdict(lambda: defaultdict(lambda: 0))
qrel = defaultdict(lambda: [])
minimal_value = []
features = []

def read_qrel():
    """function to read qrel file"""
    global qrel
    global labels

    for line in open("/Users/Sun/Documents/IR/Data/HW1/qrels.adhoc.51-100.AP89.txt"):
        try:
            qid, _, docid, label = line.split()
        except:
            continue

        label = int(label)
        qid = int(qid)
        if qid in qids:
            labels[qid][docid] = label
            qrel[qid].append(docid)

def read_trec():
    for m in models:
        try:
            trec = defaultdict(lambda: defaultdict(lambda: 0))
        except:
            continue

        min_value = float("Inf")
        for line in open("/Users/Sun/Documents/IR/HW/HW1/result/" + m + ".txt"):
            qid, _, docid, _, value, _ = line.split()
            qid = int(qid)
            value = float(value)
            trec[qid][docid] = value
            if min_value > value:
                min_value = value

        minimal_value.append(min_value)
        features.append(trec)

def build_feature_matrix(qset):
    feature_len = 0
    for qid in qset:
        feature_len += len(qrel[qid])

    feature_matrix = []
    label_matrix = []
    for i in xrange(feature_len):
        feature_matrix.append([])

    for m in xrange(len(models)):
        feature_value = features[m]
        index = 0
        for qid in qset:
            for docid in qrel[qid]:
                if docid in feature_value[qid]:
                    feat = feature_value[qid][docid]
                else:
                    feat = minimal_value[m]

                feature_matrix[index].append(feat)
                index += 1

    index = 0
    for qid in qset:
        for docid in qrel[qid]:
            label = labels[qid][docid]

            label_matrix.append(label)
            index += 1

    return feature_matrix, label_matrix

def build_result(result, qset):
    result_dict = defaultdict(lambda: defaultdict(lambda: 0))

    index = 0
    for qid in qset:
        for docid in qrel[qid]:
            result_dict[qid][docid] = result[index]
            index += 1

    return result_dict

def print_result(result_dict, output):
    of = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW6/data/" + output + ".txt", "w")
    for qid in result_dict:
        sorted_x = sorted(result_dict[qid].items(), key = operator.itemgetter(1), reverse = True)
        for item in sorted_x:
            of.write(str(qid) + ' Q0 ' + item[0] + ' 0 ' + str(item[1]) + ' Exp\n')

def assign_test():
    tempqids = qids
    random.shuffle(tempqids)
    train = tempqids[:20]
    test = tempqids[20:]

    print "Train set: ", train
    print "Test set: ", test

    return train, test

if __name__ == "__main__":
    training_set, test_set = assign_test()
    # test_set = [56, 57, 64, 71, 99]
    # training_set = [85, 59, 62, 93, 58, 77, 54, 87, 94, 100, 89, 61, 94, 68, 97, 98, 60, 80, 63, 91]

    read_qrel()
    read_trec()

    training, training_label= build_feature_matrix(training_set)
    test, test_label = build_feature_matrix(test_set)

    lr_model = linear_model.LinearRegression()
    lr_model.fit(array(training), array(training_label))

    training_score = lr_model.predict(training)
    test_score = lr_model.predict(test)

    print_result(build_result(training_score, training_set), "training")
    print_result(build_result(test_score, test_set), "test")
