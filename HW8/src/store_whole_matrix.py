from es_wrapper import es_get_term_vector, es_get_all_ids, calculate_score, es_get_text
from elasticsearch import Elasticsearch
import pickle

es = Elasticsearch()

def store_matrix():
    all_ids = es_get_all_ids(es)
    num_doc = len(all_ids)

    feature_matrix = {}
    feature_map = {}
    feature_index = 0

    count = 0
    for docid in all_ids:
        print count
        count += 1

        sparse_line = {}
        term_list = es_get_term_vector(es, docid)
        lenth = 0

        if term_list:
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

                    sparse_line[item] = score

        feature_matrix[docid] = sparse_line

    print len(feature_matrix), num_doc

    pickle.dump(feature_matrix, open("./../data/feature_matrix_whole.pkl", "w"))
    pickle.dump(all_ids, open("./../data/all_ids.pkl", "w"))
    pickle.dump(feature_map, open("./../data/feature_map.pkl", "w"))

def store_matrix_2():
    all_ids = es_get_all_ids(es)
    used_ids = []
    f_matrix = {}

    count = 0
    for did in all_ids:
        print count
        count += 1

        text = []
        term_list = es_get_term_vector(es, did)

        if term_list:
            for term in term_list:
                if term_list[term]["term_freq"] > 3:
                    for i in xrange(term_list[term]["term_freq"]):
                        text.append(term)

            if len(text) > 0:
                f_matrix[did] = ' '.join(text)
                used_ids.append(did)

        # text = es_get_text(es, did)

        # if len(text) > 1:
        #     f_matrix[did] = text
        #     used_ids.append(did)

    pickle.dump(f_matrix, open("./../data/feature_matrix_whole.pkl", "w"))
    pickle.dump(used_ids, open("./../data/all_ids.pkl", "w"))

if __name__ == "__main__":
    store_matrix_2()
