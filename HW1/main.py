from __future__ import division
from corpus import *
from elasticsearch import Elasticsearch
import operator
import math

es = Elasticsearch()
print "Start building term matrix..."
ids = query_all_ids(es)
all_doc = {}
for i in ids:
    all_doc[i] = query_vector(es, i)

def print_result(l_ranking, file, query):
    f = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW1/result/" + file, "a")
    ranking = 1
    for item in l_ranking[:1000]:
        result = str(query) + " Q0 " + item[0] + " " + str(ranking) + " " + str(item[1]) + " Exp " + "\n"
        f.write(result)
        ranking += 1

def okapi_tf(corpus):
    ranking = {}
    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in info["tf"]:
            term_scores = 0
            info = corpus["doc"].get(doc)
            for tf in info["tf"]:
                tf_score = info["tf"].get(tf)
                term_score = tf_score/(tf_score + 0.5 + (1.5*(info["length"]/corpus["avg_doc_length"])))
                score += term_score
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def tf_idf(corpus):
    ranking = {}
    rarity = {}
    for tf in corpus["df"]:
        rarity[tf] = math.log(corpus["voc_size"]/corpus["df"].get(tf))

    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in info["tf"]:
            term_scores = 0
            info = corpus["doc"].get(doc)
            for tf in info["tf"]:
                tf_score = info["tf"].get(tf)*rarity[tf]
                term_score = tf_score/(tf_score + 0.5 + (1.5*(info["length"]/corpus["avg_doc_length"])))
                score += term_score
        ranking[doc] = score

    return sorted(ranking.items(), key=operator.itemgetter(1), reverse = True)

def okapi_bm25(corpus):
    ranking = {}
    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in info["tf"]:
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
        for term in corpus["doc"][doc]["tf"]:
            corpus_frequency[term] = corpus_frequency[term] + corpus["doc"][doc]["tf"].get(term)

    for doc in corpus["doc"]:
        score = 0
        info = corpus["doc"].get(doc)
        for tf in terms["terms"]:
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

def main():
    corpus = {}
    count = 0
    for line in open("/Users/Sun/Documents/IR/Data/HW1/query.modified.txt"):
        count += 1
        query_number = int(line[:3].replace(".", ""))
        print "Calculating query No." + str(query_number) + " [" + str(count) + "/25] ..."
        terms = terms_of_query(es, line)
        corpus = build_corpus(es, terms, ids, query_term_size(es), all_doc)

        okapi_tf_ranking = okapi_tf(corpus)
        print_result(okapi_tf_ranking, "okapi_tf.txt", query_number)

        tf_idf_ranking = tf_idf(corpus)
        print_result(tf_idf_ranking, "tf_idf.txt", query_number)

        okapi_bm25_ranking = okapi_bm25(corpus)
        print_result(okapi_bm25_ranking, "okapi_bm25.txt", query_number)

        unigram_lap_ranking = unigram_lap(corpus, terms)
        print_result(unigram_lap_ranking, "unigram_lap.txt", query_number)

        unigram_jm_ranking = unigram_jm(corpus, terms)
        print_result(unigram_jm_ranking, "unigram_jm.txt", query_number)

main()
