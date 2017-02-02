from sets import Set
from page import Page
from collections import deque
import json
import re

AGING = 0.00001 # 1/90000

THRESHOLD = 10000

class Frontier:
    def __init__(self, seeds):
        self.__urls = {}
        self.__inlinks = {}
        self.__current = deque()
        for url in seeds:
            self.__urls[url] = 100000
            self.__inlinks[url] = Set()
        self.__visited = Set()
        self.__age = 0.99 #less than 1

    def currentlen(self):
        return len(self.__current)

    def restore(self, ifile):
        jstr = ifile.readline()
        jfrontier = json.loads(jstr)
        self.__urls = jfrontier["urls"]
        self.__current = deque(jfrontier["current"])
        self.__age = jfrontier["age"]
        self.__visited = Set(jfrontier["visited"])
        self.__inlinks = {}
        for item in jfrontier["inlinks"]:
            self.__inlinks[item] = Set(jfrontier["inlinks"][item])

    def next_url(self):
        if len(self.__current) == 0:
            sorted_list = sorted(self.__urls, \
                                 key=self.__urls.get, \
                                 reverse = True)
            if len(sorted_list) > THRESHOLD:
                sorted_list = sorted_list[:(len(sorted_list) / 4)]
            self.__current = deque(sorted_list)
            self.__urls = {}
        url = self.__current.popleft()
        inlinks = self.__inlinks[url]
        del self.__inlinks[url]
        self.__visited.add(url)
        yield url
        yield inlinks

    def backup(self, ofile):
        res = {}
        res["urls"] = self.__urls
        res["inlinks"] = {}
        for item in self.__inlinks:
            res["inlinks"][item] = list(self.__inlinks[item])
        res["visited"] = list(self.__visited)
        res["age"] = self.__age
        res["current"] = list(self.__current)
        strres = json.dumps(res)
        ofile.write(strres)

    def add_links(self, links, in_url):
        for url in links:
            if (re.sub(r'^http', 'https', url) not in self.__visited and \
                (re.sub(r'^http', 'https', url)) not in self.__current) \
            or (re.sub(r'^https', 'http', url) not in self.__visited and \
                (re.sub(r'^https', 'http', url)) not in self.__current):
                score = 0
                if 'harvard' in url:
                    score += 20
                if 'harvard' in in_url:
                    score += 20
                if url in self.__urls:
                    self.__urls[url] += 1
                    self.__inlinks[url].add(Page.canonical(in_url))
                else:
                    self.__urls[url] = self.__age + score
                    self.__inlinks[url] = Set()
                    self.__inlinks[url].add(Page.canonical(in_url))
                    self.__age -= AGING #no same age

    def in_link_count(self, url):
        return self.__urls.get(url)

    def empty(self):
        if self.__urls or self.__current:
            return False
        else:
            return True

    def length(self):
        return len(self.__urls)

if __name__ == "__main__":
    SEEDS = [
        "http://en.wikipedia.org/wiki/Harvard_University",
	"http://www.harvard.edu",
	"https://www.google.com/search?client=safari&rls=en&q=Harvard+costs&ie=UTF-8&oe=UTF-8",
	"https://www.cappex.com/colleges/Harvard-University/tuition-and-costs",
	"http://www.harvard.edu/harvard-glance"
    ]

    a = Frontier(SEEDS)
    print a.next_urls()
