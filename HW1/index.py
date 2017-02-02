#!/usr/bin/env python

## modified form example code
## available at https://github.com/elastic/elasticsearch-py/blob/master/example/load.py
#
from itertools import chain
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from json import dumps
import json

from parser import list_all_files, extract_docs

INDEX = "ap_dataset"
data_path = "/Users/Sun/Documents/IR/Data/HW1/ap89_collection/"

def create_index(client):
    # delete previous dataset
    client.indices.delete(index=INDEX, ignore = [400,404])

    # set mapping and settings
    MAPPING = {
        "document": {
            "properties": {
                "docno": {
                    "type": "string",
                    "store": True,
                    "index": "not_analyzed"
                },
                "text": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": "my_english"
                }
            }
        }
    }

    SETTING = {
          "settings": {
              "index": {
                  "store": {
                      "type": "default"
                  },
                  "number_of_shards": 1,
                  "number_of_replicas": 1,
                  "max_result_window" : 100000
              },
              "analysis": {
                  "analyzer": {
                      "my_english": {
                          "type": "english",
                          "stopwords_path": "stoplist.txt"
                      }
                  }
              }
          }
        }

    client.indices.create(
        index = INDEX,
        body = SETTING
    )

    client.indices.put_mapping(
        index = INDEX,
        doc_type = "document",
        body = MAPPING
    )

def parse_doc(docs):
    for d in docs:
        yield {
            '_id': d["docno"],
            'text': d["text"],
            'docno': d["docno"]
        }

def add_to_index(client):
    for f in list_all_files(data_path):
        print "Indexing file {0}".format(f)
        for ok, result in parallel_bulk(
                client,
                parse_doc(extract_docs(data_path+f)),
                index = INDEX,
                doc_type = 'document',
                thread_count = 8,
                chunk_size = 50
        ):
            action, result = result.popitem()
            if not ok:
                print "Failed to index doc"

def index():
    es = Elasticsearch()
    create_index(es)
    add_to_index(es)
    # refresh to make the documents available for search
    es.indices.refresh(index=INDEX)
    # can count the documents, should be 84678
    print "{0} documents indexed".format(es.count(index=INDEX)['count'])

if __name__ == "__main__":
    index()
