import elasticsearch

def es_get_feature(client, word):
    return client.search(
        index = 'hw7',
        doc_type = "document",
        body = {
            'query' : {
                'match' : {
                    'text' : word
                }
            },
            'size': 100000
        }
    )

def es_get_term_vector(client, did):
    res = client.termvectors(
        index = 'hw7',
        doc_type = "document",
        id = did,
        body = {
            'term_statistics' : True
        }
    )

    return res["term_vectors"]["text"]["terms"]
