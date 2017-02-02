# Retrieval Models
Wenxiang Sun
Submitted 06/09

## Objective
Implement and compare various retrieval systems using vector space models and
language models. Explain how and why their performance differs.

## Folder Structure

    ./HW2
    ├── result
    │   ├── ngram.txt
    │   ├── okapi_bm25.txt
    │   ├── okapi_tf.txt
    │   ├── out.50.txt
    │   ├── tf_idf.txt
    │   ├── unigram_jm.txt
    │   └── unigram_lap.txt
    ├── src
    │   ├── b64.py
    │   ├── b64.pyc
    │   ├── corpus.py
    │   ├── corpus.pyc
    │   ├── index.py
    │   ├── index.pyc
    │   ├── index_builder.py
    │   ├── main.py
    │   ├── parser.py
    │   ├── parser.pyc
    │   ├── query.py
    │   ├── query.pyc
    │   ├── requirement.txt
    │   ├── test.py
    │   ├── tokenizer.py
    │   └── tokenizer.pyc
    ├── summary.md
    └── trec_eval

    2 directories, 25 files


## Result

Index Type: Stemmed With Stopwords
Index Size: 145.2M with term position

| model           | Ave Precision  | Ave Precision (es)|
| --------------- | -------------- | ----------------- |
| Okapi Tf        | 0.2056         | 0.2051            |
| Tf idf          | 0.2022         | 0.1880            |
| Okapi BM25      | 0.2504         | 0.2634            |
| Unigram Laplace | 0.2127         | 0.2194            |
| Unigram JM      | 0.2050         | 0.2178            |
| Proximity       | 0.1470         | N/A               |
