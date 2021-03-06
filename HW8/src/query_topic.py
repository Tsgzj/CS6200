from collections import defaultdict
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import pickle
import operator

def read_doc_list():
    doc_list = defaultdict(lambda: [])
    used_ids = set(pickle.load(open("./../data/all_ids.pkl")))

    for line in open("./../data/doc_list"):
        qid, docid = line.split()

        if docid in used_ids:
            doc_list[qid].append(docid)

    print len(doc_list)
    return doc_list

def build_matrix(doc_list):
    all_matrix = pickle.load(open("./../data/feature_matrix_whole.pkl"))

    data_list = []
    for doc in doc_list:
        data_list.append(all_matrix[doc])

    return data_list

def print_top_words(model, feature_names, of):
    topic_score_list = {}
    topic_feature_list = {}
    for topic_idx, topic in enumerate(model.components_):
        total_score = sum(topic)
        topic_score_list[topic_idx] = total_score
        topic_feature_list[topic_idx] = topic

    sorted_topic = sorted(topic_score_list.items(), key = operator.itemgetter(1), reverse=True)

    for item in sorted_topic:
        of.write("Topic #%d:\n" % item[0])
        for i in topic_feature_list[item[0]].argsort()[:-30 -1:-1]:
            of.write(feature_names[i] + " " + str(topic_feature_list[item[0]][i]) + " ")
        of.write("\n")

def print_doc_topic(doc_topic, doc_list, query, of):
    for i in xrange(len(doc_list[query])):
        of.write("Doc #%s:\n" % doc_list[query][i])
        for ti in doc_topic[i].argsort()[:-3 - 1:-1]:
            of.write("topic " + str(ti) + " ")
        of.write("\n")

if __name__ == "__main__":
    doc_list = read_doc_list()
    doc_id = read_doc_list()

    for query in doc_list:
        print len(doc_list[query])
        data_samples = build_matrix(doc_list[query])

        tfidf_vectorizer = TfidfVectorizer(max_df=0.95, min_df=2, #max_features=n_features,
                                   stop_words='english')
        tfidf = tfidf_vectorizer.fit_transform(data_samples)
        tfidf_feature_names = tfidf_vectorizer.get_feature_names()

        lda = LatentDirichletAllocation(n_topics=20, max_iter=5,
                                learning_method='online', learning_offset=50.,
                                random_state=0)
        lda.fit(tfidf)

        of = open("./../data/query_result.txt", "a")
        of.write("Query " + str(query) + "\n")
        print_top_words(lda, tfidf_feature_names, of)
        of.write("\n")

        doc_topic = lda.transform(tfidf)
        of2 = open("./../data/doc_result.txt", "a")
        of2.write("Query " + str(query) + "\n")
        print_doc_topic(doc_topic, doc_id, query, of2)
        of2.write("\n")
