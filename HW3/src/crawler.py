import urllib2
import httplib2
import socket
from urlparse import urlparse
from page import Page
from reppy.cache import RobotsCache
import threading
import Queue
import urllib3

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0"
HEADERS = {'User-Agent': UA}
urllib3.disable_warnings()

class Crawler:
    def __init__(self):
        self.__robot = {}
        self.__http = urllib3.PoolManager()

    def allowed(self, url):
        surl = urlparse(url)
        rurl = surl.scheme + '://' + surl.hostname + '/robots.txt'
        if rurl in self.__robot:
            if not self.__robot[rurl].expired:
                return self.__robot[rurl].allowed(url, UA)
        try:
            r = RobotsCache().fetch(rurl)
        except:
            return False
        else:
            self.__robot[rurl] = r
            # add a rule object
            return self.__robot[rurl].allowed(url, UA)

    def fetch(self, url, inlinks):
        if self.allowed(url):
            try:
                f = self.__http.request('Get', url, timeout = 2)
            except socket.timeout:
                return Page()
            except:
                return Page()
            else:
                try:
                    header = f.headers
                except:
                    header = None

                try:
                    ctype = header['Content-Type']
                except:
                    ctype = None

                try:
                    lang = header['Content-Language']
                except:
                    lang = None
                if ctype and lang:
                    if 'text/html' in ctype and 'en' in lang:
                        try:
                            html = f.data
                        except:
                            print "Cannot read"
                            return Page()
                        else:
                            rhead = {}
                            for item in header:
                                rhead[item] = header[item]
                            return Page(url, rhead, html, inlinks, True)
                    else:
                        print "Not english or html"
                        return Page()
                elif ctype:
                    if 'text/html' in ctype:
                        try:
                            html = f.data
                        except:
                            print "Cannot read"
                            return Page()
                        else:
                            rhead = {}
                            for item in header:
                                rhead[item] = header[item]
                            return Page(url, rhead, html, inlinks, True)
                    else:
                        print "Not html"
                        return Page()
                else:
                    print "No header"
                    return Page()
        else:
            print "Robots not allowed"
            return Page()

if __name__ == "__main__":
    a = Crawler()
    b = a.fetch("http://www.harvard.edu")
    if b:
        print len(b.links())
