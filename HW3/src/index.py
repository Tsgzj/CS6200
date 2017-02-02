#!/usr/bin/env python

## modified form example code
## available at https://github.com/elastic/elasticsearch-py/blob/master/example/load.py
#
import os
import base64
from multiprocessing import Pool
from elasticsearch import Elasticsearch

from pageparser import parse_file
from sets import Set

INDEX = "test"
DATA_PATH = "/Users/Sun/Documents/IR/Data/HW3/pages/"


es = Elasticsearch(timeout=100)

def list_all_files(file_dir):
    return [f for f in os.listdir(file_dir) if f.startswith("page")]

def create_index(client):
    # delete previous dataset
    client.indices.delete(index=INDEX, ignore=[400, 404])

    # set mapping and settings
    MAPPING = {
        "document": {
            "properties": {
                "docno": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads"
                },
                "HTTPheader": {
                    "type": "string",
                    "store": True,
                    "index": "not_analyzed"
                },
                "title":{
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads"
                },
                "text": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads"
                },
                "html_Source": {
                    "type":"string",
                    "store": True,
                    "index": "no"
                },
                "out_links":{
                    "type": "string",
                    "store": True,
                    "index": "no"
                },
                "author":{
                    "type": "string",
                    "store": True,
                    "index": "analyzed"
                }
            }
        }
    }

    SETTING = {
        "settings":{
            "index":{
                "store":{
                    "type":"default"
                },
                "number_of_shards":3,
                "number_of_replicas":0
            }
        }
    }

    client.indices.create(
        index=INDEX,
        body=SETTING
    )

    client.indices.put_mapping(
        index=INDEX,
        doc_type="document",
        body=MAPPING
    )

def parse_doc(d):
    return {
        'text': d["text"],
        'docno': d["docno"],
        'HTTPheader': d["HTTPheader"],
        'title': d["title"],
        'in_links': d["inlinks"],
        'out_links': d["outlinks"],
        'author': d["author"],
        'html_Source': d["html_Source"]
    }

# def add_to_index(f):
#     print "Indexing file {0}".format(f)
#     for ok, result in parallel_bulk(
#             client,
#             parse_doc(parse_file(DATA_PATH+f)),
#             index = INDEX,
#             doc_type = 'document',
#             thread_count = 8,
#             chunk_size = 50
#     ):
#         action, result = result.popitem()
#         if not ok:
#             print "Failed to index doc"

def index(ifile):
    print "Indexing " + str(ifile)
    mbody = parse_file(DATA_PATH + ifile)
    for item in mbody:
        __id = base64.urlsafe_b64encode(item["docno"])
        try:
            pre = es.get(
                index='test',
                doc_type='document',
                id=__id
            )

            item["author"] = str(pre['_source']['author']) + ";" + item["author"]
            item["in_links"] = list(Set(pre['_source']['in_links'] + item["inlinks"]))

            es.update(
                index='test',
                doc_type='document',
                id=__id,
                body={
                    "doc":{
                        "in_links": item["in_links"],
                        "author": item["author"]
                    }
                }
            )
            print "Updated"
        except Exception, e:
            es.index(
                index='test',
                doc_type='document',
                id=__id,
                body=parse_doc(item)
            )
#     # add_to_index(es)
    # refresh to make the documents available for search
    # es.indices.refresh(index=INDEX)
    # can count the documents, should be 84678
    # print "{0} documents indexed".format(es.count(index=INDEX)['count'])



if __name__ == "__main__":
    p = Pool(8)
    f_list = list_all_files(DATA_PATH)
    p.map(index, f_list)
    # for ifile in f_list:
    #     index(ifile)
#     es.update(
#         index='test',
#         doc_type='document',
#         id='aHR0cHM6Ly9tYWdhemluZS5obXMuaGFydmFyZC5lZHUvaXNzdWU9MTE1',
#         body={
#             'doc':{
#                 'author':'Wen;Wen;Wenxang'
#             }
#         }
#   )
