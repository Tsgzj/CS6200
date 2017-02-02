from __future__ import division
from corpus import *
import operator
from index import Index
from collections import deque
import math


def print_result(l_ranking, file, query, client):
    f = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW2/result/" + file, "a")
    ranking = 1
    for item in l_ranking[:1000]:
        result = str(query) + " Q0 " + client.get_doc_name(item[0]) + " " + str(ranking) + " " + str(item[1]) + " Exp " + "\n"
        f.write(result)
        ranking += 1

def okapi_tf(corpus, terms):
    ranking = {}
    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in terms["terms"]:
            if tf in info["tf"]:
                tf_score = info["tf"].get(tf)
                term_score = tf_score/(tf_score + 0.5 + (1.5*(info["length"]/corpus["avg_doc_length"])))
                score += term_score
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def tf_idf(corpus, terms):
    ranking = {}
    rarity = {}
    for tf in corpus["df"]:
        rarity[tf] = math.log(corpus["voc_size"]/corpus["df"].get(tf))

    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in terms["terms"]:
            if tf in corpus["df"]:
                rarity[tf] = math.log(corpus["voc_size"]/corpus["df"].get(tf))
            else:
                rarity[tf] = 0

            if tf in info["tf"]:
                tf_score = info["tf"].get(tf)*rarity[tf]
                term_score = tf_score/(tf_score + 0.5 + (1.5*(info["length"]/corpus["avg_doc_length"])))
                score += term_score
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def okapi_bm25(corpus, terms):
    ranking = {}
    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in terms["terms"]:
            if tf in info["tf"]:
                # okapi_bm25
                # k1 = 1.2, k2 = 500, b = 0.75
                tf_score = info["tf"].get(tf)
                k1 = 1.2
                k2 = 500
                b = 0.1
                term_score = (tf_score + k1 * tf_score) / (tf_score + k1 * ((1 - b) + b * (info["length"]/corpus["avg_doc_length"]))) \
                             * math.log((corpus["voc_size"] + 0.5) / (corpus["df"][tf] + 0.5))
                # the way I trim query makes (tf(i,q) + k2 * tf(i,q)) / (tf_i_q + k2) = 1
                # * (tf_score + k2 * tf_score) / (tf_score + k2) \
                score += term_score
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def unigram_lap(corpus, terms):
    ranking = {}
    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in terms["terms"]:
            if tf in info["tf"]:
                tf_score = info["tf"].get(tf)
            else:
                tf_score = 0
            p_laplace = (tf_score + 1)/(info["length"] + corpus["voc_size"])
            score += math.log(p_laplace)
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def unigram_jm(corpus, terms):
    ranking = {}
    total_doc_length = 0
    corpus_frequency = {}
    for term in terms["terms"]:
        corpus_frequency[term] = 0

    for doc in corpus["doc"]:
        total_doc_length += corpus["doc"].get(doc).get("length")
        for term in terms["terms"]:
            if term in corpus["doc"][doc]["tf"]:
                corpus_frequency[term] = corpus_frequency[term] + corpus["doc"][doc]["tf"].get(term)

    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in terms["terms"]:
            if corpus_frequency.get(tf) != 0:
                if tf in info["tf"]:
                    fore_ground = info["tf"].get(tf) / info["length"]
                else:
                    fore_ground = 0
                _lambda = info["length"] / (info["length"] + corpus["avg_doc_length"])
                back_ground = corpus_frequency.get(tf) / total_doc_length
                term_score = math.log(_lambda * fore_ground + (1 - _lambda) * back_ground)
                score += term_score
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def ngram(corpus, terms):
    # score = 0.8^((s-k)/k) + hit_count
    ranking = {}
    for doc in corpus["doc"]:
        score = 0
        pos_list = []
        info = corpus["doc"].get(doc)
        hit_count = 0
        for t in terms["terms"]:
            if t in info["pos"]:
                pos_list.append(info["pos"].get(t))
                hit_count += 1
        if len(pos_list) > 0:
            s = min_dis(pos_list)
            score += hit_count + math.pow(0.8, s)
        ranking[doc] = score
    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def min_dis(doc):
    m = []
    for i in doc:
        m.append(deque(i))

    min_distance = float('Inf')
    flag = False
    for i in m:
        if not len(i) <= 1:
            flag = True
    while flag:
        flag = False
        current = []
        for i in m:
            current.append(i[0])
        temp = max(current) - min(current)
        if temp < min_distance:
            min_distance = temp
        min_index = current.index(min(current))
        while (len(m[min_index]) < 2 and len(current) > 1):
            current[min_index] = max(current) + 1
            min_index = current.index(min(current))
        m[min_index].popleft()
        for i in m:
            if not len(i) <= 1:
                flag = True
    return min_distance

def main():
    i = Index()
    print "Start building term matrix..."
    ids = query_all_ids(i)
    corpus = {}
    count = 0
    all_terms = set()
    all_query = []
    for line in open("/Users/Sun/Documents/IR/Data/HW1/query.modified.txt"):
        terms = terms_of_query(i, line)
        all_query.append(terms)
        for t in terms["terms"]:
            all_terms.add(t)

    corpus = build_corpus(i, all_terms, ids, query_term_size(i))

    for query in all_query:
        count += 1
        print "Calculating query No." + str(query["id"]) + " [" + str(count) + "/25] ..."

        okapi_tf_ranking = okapi_tf(corpus, query)
        print_result(okapi_tf_ranking, "okapi_tf.txt", query["id"], i)

        tf_idf_ranking = tf_idf(corpus, query)
        print_result(tf_idf_ranking, "tf_idf.txt", query["id"], i)

        okapi_bm25_ranking = okapi_bm25(corpus, query)
        print_result(okapi_bm25_ranking, "okapi_bm25.txt", query["id"], i)

        unigram_lap_ranking = unigram_lap(corpus, query)
        print_result(unigram_lap_ranking, "unigram_lap.txt", query["id"], i)

        unigram_jm_ranking = unigram_jm(corpus, query)
        print_result(unigram_jm_ranking, "unigram_jm.txt", query["id"], i)

        ngram_ranking = ngram(corpus, query)
        print_result(ngram_ranking, "ngram.txt", query["id"], i)

main()
# print min_dis([[101], [47, 131, 308, 363], [181]])
