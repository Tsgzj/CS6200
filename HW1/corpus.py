#!/usr/bin/env python
from query import *
from elasticsearch import Elasticsearch

def build_corpus(client, terms, ids, vec_size, all_doc):
    corpus = {"df": {}, "doc": {}, "doc_count": None}
    ########################################
    # the structure of the corpus will be: #
    # corpus = {                           #
    #     'df': {                          #
    #         'term1': df_1,               #
    #         'term2': df_2,               #
    #         'term3': df_3...             #
    #     },                               #
    #     'doc': {                         #
    #         'id_1': {                    #
    #             length: len_1,           #
    #             tf: {                    #
    #                 'term1': tf_1,       #
    #                 'term2': tf_2,       #
    #                 'term3': tf_3...     #
    #             }                        #
    #         }                            #
    #     },                               #
    #     'doc_count': number of docs      #
    #     'avg_doc_length':                #
    #     'voc_size':                      #
    # }                                    #
    ########################################

    for term in terms["terms"]:
        res = query_term(client, term)
        corpus["df"][term] = len(res)
    #     for doc in res:
    #         if not doc["id"] in corpus["doc"]:
    #             corpus["doc"][doc["id"]] = {"tf": {}, "length": length_of_doc(client, doc["id"])}
    #         corpus["doc"][doc["id"]]["tf"][term] = doc["score"]

    for i in ids:
        corpus["doc"][i] = {"tf": {}, "length": None}
        resp = all_doc.get(i)

        length = 0
        relative_terms = {}
        for t in resp["tf"]:
            length += resp["tf"].get(t)
            if t in terms["terms"]:
                relative_terms[t] = resp["tf"].get(t)

        corpus["doc"][i]["length"] = length
        corpus["doc"][i]["tf"] = relative_terms

    corpus["doc_count"] = len(corpus["doc"])
    total_doc_length = 0
    for doc in corpus["doc"]:
        total_doc_length += corpus["doc"].get(doc)["length"]
    corpus["avg_doc_length"] = total_doc_length/corpus["doc_count"]
    corpus["voc_size"] = vec_size
    return corpus
