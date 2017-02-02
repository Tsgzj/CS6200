from lxml import etree
from urlparse import urlparse, urljoin
from readability.readability import Document
import re
import validators
import json
from sets import Set
# import urllib2 #used for test

class Page:
    def __init__(self, url = None, header = None, body = None, inlinks = None, fetched = False):
        self.__url = url
        self.__inlinks = inlinks
        # self.__body = self.clean(body)
        self.__header = header
        self.__body = body
        self.__fetched = fetched
        self.__sqz = re.compile(r'\/\/+')

    def inlinks(self):
        inlinks = []
        for item in self.__inlinks:
            inlinks.append(item)
        return inlinks

    def links(self):
        links = Set()
        try:
            tree = etree.HTML(self.__body)
        except:
            return links
        else:
            try:
                l = tree.xpath('//a/@href')
            except:
                return links
            else:
                for item in tree.xpath('//a/@href'):
                    realurl = self.absolute_link(item)
                    if realurl and self.isutf8(realurl):
                        if validators.url(realurl):
                            links.add(realurl)
                return links

    def canonicalize(self, url):
        if url:
            try:
                surl = urlparse(url)
            except:
                print "Illegal url"
                return None
            else:
                if surl.hostname and surl.path:
                    url = surl.scheme.lower() + '://' + \
                          surl.hostname.lower() + self.__sqz.sub('/', surl.path) + surl.params + surl.query
                elif surl.path:
                    url = surl.scheme.lower() + '://' + \
                          self.__sqz.sub('/', surl.path) + surl.params + surl.query
                elif surl.hostname:
                    url = surl.scheme.lower() + '://' + \
                          surl.hostname.lower() + surl.params + surl.query
                else:
                    url = url
                if url.endswith('/'):
                    return url[:-1]
                else:
                    return url
        else:
            return None

    def isutf8(self, url):
        try:
            url.decode('utf-8')
        except:
            return False
        else:
            return True

    def absolute(self, url):
        try:
            surl = urlparse(url)
        except:
            print "Ilegal url"
        else:
            if surl.scheme:
                return url
            else:
                return urljoin(self.__url, url)

    def absolute_link(self, url):
        return self.canonicalize(self.absolute(url))

    def dump(self, ofile):
        res = {}
        res["url"] = self.__url
        res["raw"] = self.__body
        res["header"] = self.__header
        res["inlinks"] = list(self.__inlinks)
        res["outlinks"] = list(self.links())
        try:
            strres = json.dumps(res) + '\n'
        except:
            print "Enconding error, will not dump"
        else:
            ofile.write(strres)

    @staticmethod
    def domain(url):
        if url:
            purl = urlparse(url)
            if purl.scheme and purl.hostname:
                return purl.scheme + '://' + purl.hostname
            else:
                return None
        else:
            return None

    @staticmethod
    def canonical(url):
        surl = urlparse(url)
        url = surl.scheme.lower() + '://' + \
              surl.hostname.lower() + re.compile(r'\/\/+').sub('/', surl.path)
        if url.endswith('/'):
            return url[:-1]
        else:
            return url

    def fetched(self):
        return self.__fetched

if __name__ == "__main__":
    url = "http://www.harvard.edu"
    req = urllib2.Request(url, headers={'User-Agent' : \
                                        "Mozilla/5.0 (Macintosh; \
                                        Intel Mac OS X 10.11; rv:47.0) \
                                        Gecko/20100101 Firefox/47.0"})
    con = urllib2.urlopen( req )
    a = Page(url, con.read())
    print a.links()
    print a.host()
    print len(a.links())
