from __future__ import division
import random
import pickle
import itertools
import math
import operator
from scipy.sparse import lil_matrix
from scipy.io import mmwrite, mmread
from collections import defaultdict
from elasticsearch import Elasticsearch
from index import list_all_files_start_with, parse_index
from es_wrapper import es_get_feature, es_get_term_vector
from stemming.porter import stem
from liblinearutil import *
from sklearn import linear_model
from numpy import array

es = Elasticsearch()
DIR = "/Users/Sun/Documents/IR/Data/hw7/"

def get_feature_list():
    feature_list = {}
    index = 0
    for line in open("./../data/all_dic.txt"):
    # for line in open("./../data/featurelist.txt"):
        word = line.strip()
        feature_list[word] = index

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
    # all_ids = list_all_files_start_with(DIR + "trec/data/", "inmail")
    all_ids = query_all_ids()
    matrix = {}
    for item in all_ids:
        matrix[item] = {"label": None, "feature": {}}

    print "Num of files " + str(len(matrix))
    return matrix

def assign_test():
    # all_ids = list_all_files_start_with(DIR + "trec/data/", "inmail")
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
    #     ofe.write(e+"\n")

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
    count = 0
    for word in features:
        print count
        count += 1
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

def feature_involved(qset, origin_matrix):
    feature_in_training = set()
    for item in qset:
        for feature in origin_matrix[item]["feature"]:
            feature_in_training.add(feature)

    return list(feature_in_training)

def calculate_score(tf, num_doc, doc, l):
    idf = 1 + math.log(num_doc/(doc + 1))
    tf = math.sqrt(tf)
    norm = 1 / math.sqrt(l)

    return tf * idf * norm


def build_feature_sparse_matrix(qset, num_doc):
    feature_matrix = []
    label_matrix = []

    feature_map = {}
    feature_index = 0
    index = parse_index("/Users/Sun/Documents/IR/Data/HW7/trec/full/index")
    for i in xrange(len(qset)):
        print i
        current_id = qset[i]
        print current_id
        if current_id in index:
            label_matrix.append(0)
        else:
            label_matrix.append(1)

        sparse_line = {}

        term_list = es_get_term_vector(es, current_id)
        lenth = 0
        for item in term_list:
            lenth += term_list[item]["term_freq"]

        for item in term_list:
            tf = term_list[item]["term_freq"]
            doc = term_list[item]["doc_freq"]

            score = calculate_score(tf, num_doc, doc, lenth)

            if item in feature_map:
                feature_id = feature_map[item]
            else:
                feature_map[item] = feature_index
                feature_id = feature_index
                feature_index += 1

            sparse_line[feature_id] = score

        feature_matrix.append(sparse_line)

    sparse_feature_matrix = lil_matrix((len(qset), len(feature_map)))
    print len(qset), len(feature_map)
    l_index = 0
    for line in feature_matrix:
        print l_index
        for item in line:
            sparse_feature_matrix[l_index, item] = line[item]
        l_index += 1

    return sparse_feature_matrix.tocsr(), label_matrix, feature_map

def build_test_feature_sparse_matrix(qset, feature_map, num_doc):
    feature_matrix = lil_matrix((len(qset), len(feature_map)))
    label_matrix = []

    index = parse_index("/Users/Sun/Documents/IR/Data/HW7/trec/full/index")
    for i in xrange(len(qset)):
        print i
        current_id = qset[i]

        if current_id in index:
            label_matrix.append(0)
        else:
            label_matrix.append(1)

        term_list = es_get_term_vector(es, current_id)
        lenth = 0
        for item in term_list:
            lenth += term_list[item]["term_freq"]

        for item in term_list:
            if item in feature_map:
                tf = term_list[item]["term_freq"]
                doc = term_list[item]["doc_freq"]

                score = calculate_score(tf, num_doc, doc, lenth)

                feature_matrix[i, feature_map[item]] = score

    return feature_matrix.tocsr(), label_matrix


def build_result(result, prob, qset):
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

    # ori, fea = build_matrix()
    # fea_involved = feature_involved(training_set, ori)

    # training, training_label, feature_map = build_feature_sparse_matrix(training_set, len(training_set) + len(test_set))
    # mmwrite("./../data/training_data", training)
    # pickle.dump(training_label, open("./../data/training_label", "w"))
    # pickle.dump(feature_map, open("./../data/feature_map", "w"))

    # test, test_label = build_test_feature_sparse_matrix(test_set, feature_map, len(training_set) + len(test_set))
    # mmwrite("./../data/test_data", test)
    # pickle.dump(test_label, open("./../data/test_label", "w"))

    training_label = pickle.load(open("./../data/training_label"))
    training = mmread("./../data/training_data")
    test_label = pickle.load(open("./../data/test_label"))
    test = mmread("./../data/test_data")
    print "Loading finished"

#     prob = problem(training_label, training)
#     param = parameter('-s 0')
#     m = train(prob, param) # m is a ctype pointer to a model
#     test_score, acc, test_prob = predict(test_label, test, m, '-b 1')

    lr_model = linear_model.LogisticRegression()
    lr_model.fit(training, training_label)

    of = open("./../data/coef_allunigram.txt", "w")
    feature_map = pickle.load(open("./../data/feature_map"))
    sorted_feature = sorted(feature_map.items(), key=operator.itemgetter(1))
    coef_map = {}
    for f_index in xrange(len(sorted_feature)):
        print lr_model.coef_[0][f_index]
        coef_map[sorted_feature[f_index][0]] = lr_model.coef_[0][f_index]

    print "Coef map finished"
    sorted_coef_map = sorted(coef_map.items(), key=operator.itemgetter(1), reverse = True)
    for i in sorted_coef_map[:50]:
        of.write(str(i[0]) + " " + str(i[1]) + "\n")

    test_score = lr_model.predict(test)
    test_prob = lr_model.predict_proba(test)

    print_result(build_result(test_score, test_prob, test_set), "all_unigram")
