import random
import operator
from sklearn import linear_model
from sklearn.naive_bayes import GaussianNB
from collections import defaultdict
from numpy import array
from elasticsearch import Elasticsearch
from index import list_all_files_start_with, parse_index
from es_wrapper import es_get_feature
from stemming.porter import stem
# from liblinearutil import *

DIR = "/Users/Sun/Documents/IR/Data/HW7/trec/data/"
es = Elasticsearch()

def get_feature_list():
    feature_list = set()
    for line in open("./../data/featurelist.txt"):
        word = line.strip()
        feature_list.add(stem(word.lower()))

    return feature_list

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

def init_matrix():
    # all_ids = list_all_files_start_with(DIR, "inmail")
    all_ids = query_all_ids()
    matrix = {}
    for item in all_ids:
        matrix[item] = {"label": None, "feature": {}}

    print "Num of files " + str(len(matrix))
    return matrix

def assign_test():
    # all_ids = list_all_files_start_with(DIR, "inmail")
    # all_ids = query_all_ids()
    # temp_ids = all_ids
    # random.shuffle(temp_ids)
    # slice_num = int(len(all_ids) * 0.8)
    # train = temp_ids[: slice_num]
    # test = temp_ids[slice_num :]

    # oft = open("./../data/train_list.txt", "w")
    # ofe = open("./../data/test_list.txt", "w")
    # for t in train:
    #     oft.write(t+"\n")

    # for e in test:
    #     ofe.write(t+"\n")

    train = []
    test = []

    for line in open("./../data/train_list.txt"):
        train.append(line.strip())
    for line in open("./../data/test_list.txt"):
        test.append(line.strip())

    return train, test

def build_matrix():
    features = get_feature_list()
    print len(features)
    matrix = init_matrix()
    for word in features:
        res = es_get_feature(es, word)
        for item in res["hits"]["hits"]:
            matrix[item["_id"]]["feature"][word] = item["_score"]

    print "Extracted all features"
    index = parse_index("/Users/Sun/Documents/IR/Data/HW7/trec/full/index")
    for item in matrix:
        if item in index:
            matrix[item]["label"] = 0
        else:
            matrix[item]["label"] = 1

    return matrix, features

def build_feature_matrix(qset, features, origin_matrix):
    feature_matrix = []
    label_matrix = []


    for i in xrange(len(qset)):
        feature_matrix.append([])
        current_id = qset[i]
        values = origin_matrix[current_id]

        label_matrix.append(values["label"])
        for f in features:
            if f in values["feature"]:
                score = values["feature"][f]
            else:
                score = 0

            feature_matrix[i].append(score)

    return feature_matrix, label_matrix

def build_result(result, prob, qset, origin_matrix):
    result_dict = defaultdict(lambda: defaultdict(lambda: ()))

    index = 0
    for item in qset:
        result_dict[item] = (result[index], max(prob[index]))
        index += 1

    return result_dict

def print_result(result_dict, output):
    of = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW7/data/" + output + ".txt", "w")
    for item in result_dict:
        of.write(item + " " + str(result_dict[item][0]) + " " + str(result_dict[item][1]) + "\n")

if __name__ == "__main__":
    training_set, test_set = assign_test()

    ori, fea = build_matrix()

    training, training_label = build_feature_matrix(training_set, fea, ori)
    test, test_label = build_feature_matrix(test_set, fea, ori)

    lr_model = linear_model.LogisticRegression()
    # # lr_model = GaussianNB()
    lr_model.fit(array(training), array(training_label))

    of = open("./../data/coef.txt", "w")
    feature_list = list(get_feature_list())
    coef_map = {}
    for f_index in xrange(len(feature_list)):
        print lr_model.coef_[0][f_index]
        coef_map[feature_list[f_index]] = lr_model.coef_[0][f_index]

    print "Coef map finished"
    sorted_coef_map = sorted(coef_map.items(), key=operator.itemgetter(1), reverse = True)
    for i in sorted_coef_map[:50]:
        of.write(str(i[0]) + " " + str(i[1]) + "\n")

    test_score = lr_model.predict(test)
    test_prob = lr_model.predict_proba(test)

    print_result(build_result(test_score, test_prob, test_set, ori), "mannual")

    # prob = problem(training_label, training)
    # param = parameter('-s 0')
    # m = train(prob, param) # m is a ctype pointer to a model
    # label, acc, val = predict(test_label, test, m, '-b 1')

    # print_result(build_result(label, acc, test, ori), "man.txt")
