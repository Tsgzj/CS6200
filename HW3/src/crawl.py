from time import sleep
import datetime
import threading
from crawler import Crawler
from page import Page
from frontier import Frontier

SEEDS = [
    "http://en.wikipedia.org/wiki/Harvard_University",
    "http://www.harvard.edu",
    "https://www.google.com/search?client=safari&rls=en&q=Harvard+costs&ie=UTF-8&oe=UTF-8",
    # apparently google doesn't allowed crawlling on google.com/search
    "https://www.cappex.com/colleges/Harvard-University/tuition-and-costs",
    "http://www.harvard.edu/harvard-glance",
    "http://www.hbs.edu/Pages/default.aspx",
    "http://college.harvard.edu",
    "http://www.dce.harvard.edu",
    "http://hsdm.harvard.edu",
    "http://www.fas.harvard.edu",
    "http://hds.harvard.edu",
    "http://www.gsd.harvard.edu",
    "http://www.gse.harvard.edu",
    "http://www.gsas.harvard.edu",
    "http://www.seas.harvard.edu",
    "https://www.hks.harvard.edu",
    "http://hls.harvard.edu",
    "http://www.radcliffe.harvard.edu",
    "http://hms.harvard.edu",
    "https://www.hsph.harvard.edu"
]

frontier = Frontier(SEEDS)
crawler = Crawler()

FILE = "/Users/Sun/Documents/IR/Data/HW3/pages/page"
FRONTIER_BACKUP = "/Users/Sun/Documents/IR/Data/HW3/pages/frontier"
# frontier.restore(open(FRONTIER_BACKUP))

crawled = 0
MIN_CRAWL = 35000

purl = None
DOMAIN_TIMESTAMP = {}

while not frontier.empty() and crawled < MIN_CRAWL:
    if crawled % 100 == 0:
        print str(crawled) + " pages crawled   " + str(crawled * 100 / MIN_CRAWL) + '%'
        ofile = open(FILE + str(crawled / 100), 'a')
        frontier.backup(open(FRONTIER_BACKUP, 'w'))

    url, inlinks = frontier.next_url()
    domain = Page.domain(url)
    now = datetime.datetime.now()
    print "Fetching " + url
    if domain in DOMAIN_TIMESTAMP:
        elasp = now - DOMAIN_TIMESTAMP[domain]
        ELASP_IN_SEC = elasp.total_seconds()
        if ELASP_IN_SEC < 1:
            sleep(1 - ELASP_IN_SEC)

    page = crawler.fetch(url, inlinks)
    DOMAIN_TIMESTAMP[domain] = now
    if page.fetched():
        outlinks = page.links()
        t=threading.Thread(target = page.dump,args=(ofile,))
        t.start()
        if (32000 - crawled) > frontier.currentlen():
            frontier.add_links(outlinks, url)
        crawled += 1

print str(crawled) + " pages crawled"
