from elasticsearch import Elasticsearch

queries = {"151601": "Harvard famous alumni", "151602": "Harvard rankings", "151603": "Harvard costs"}
es = Elasticsearch()

def write_trec(res):
    of = open('/Users/Sun/dropbox/CS6200_W_Sun/hw5/data/trec_' + res[0]['qid'] + '.txt', 'w')
    for i in xrange(len(res)):
        of.write(res[i]['qid'] + ' Q0 ' +  res[i]['id'] +
                 ' ' + str(i + 1) + ' ' + str(res[i]['score']) + ' exp\n')

def get_res_list(query, qid):
    try:
        resp = es.search(
            index = 'test',
            doc_type = 'document',
            body = {
                'query':{
                    'query_string': {
                        'query': query.lower()
                    }
                },
                'fields' : [],
                'size': 200
            }
        )
    except:
        return []

    res = []
    for item in resp["hits"]["hits"]:
        temp = {}
        temp['id'] = item['_id']
        temp['qid'] = qid
        temp['score'] = item['_score']
        res.append(temp)
    return res

if __name__ == "__main__":
    for q in queries:
        res = get_res_list(queries[q], q)
        write_trec(res)
