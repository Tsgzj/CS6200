#!/usr/bin/env python
from query import *
from index import Index

def build_corpus(client, terms, ids, vec_size):
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
    #             pos: {                   #
    #                 'term1': []          #
    #             }                        #
    #         }                            #
    #     },                               #
    #     'doc_count': number of docs      #
    #     'avg_doc_length':                #
    #     'voc_size':                      #
    # }                                    #
    ########################################

    for term in terms:
        res = query_term(client, term)
        corpus["df"][term] = len(res)

        for doc in res:
            if doc["id"] in corpus["doc"]:
                corpus["doc"][doc["id"]]["tf"][term] = len(doc["pos"])
                corpus["doc"][doc["id"]]["pos"][term] = doc["pos"]
            else:
                corpus["doc"][doc["id"]] = {"tf": {}, "pos": {}, "length": None}
                corpus["doc"][doc["id"]]["tf"][term] = len(doc["pos"])
                corpus["doc"][doc["id"]]["pos"][term] = doc["pos"]

    for i in ids:
        if not i in corpus["doc"]:
            corpus["doc"][i] = {"tf": {}, "pos": {}, "length": None}
        corpus["doc"][i]["length"] = client.length_of_doc(i)
        # corpus["doc"][i]["tf"] = relative_terms

    corpus["doc_count"] = len(corpus["doc"])
    total_doc_length = 0
    for doc in corpus["doc"]:
        total_doc_length += corpus["doc"].get(doc)["length"]
    corpus["avg_doc_length"] = total_doc_length/corpus["doc_count"]
    corpus["voc_size"] = vec_size
    return corpus
