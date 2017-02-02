import operator
from page_rank import page_rank

wt2g = "/Users/Sun/Documents/IR/Data/HW4/wt2g_inlinks.txt"

def build_graph(myfile):
    graph = {}

    f = open(myfile)
    for line in f:
        l = line.split(None, 1)
        graph[l[0]] = {'inlinks': [], 'outlinks': 0}

    f = open(myfile)
    for line in f:
        l = line.split(None, 1)
        if len(l) > 1:
            for ilk in l[1].split():
                if ilk in graph:
                    graph[l[0]]['inlinks'].append(ilk)

    for item in graph:
        for ilk in graph[item]['inlinks']:
            graph[ilk]['outlinks'] += 1

    print "Finish building graph"
    return graph

if __name__ == '__main__':
    wg = build_graph(wt2g)
    rank = page_rank(wg)

    sorted_rank = sorted(rank.items(), key = operator.itemgetter(1), reverse = True)

    outf = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW4/data/wt2g_pagerank.txt", "a")

    for i in range(500):
        res = sorted_rank[i][0] + " " + str(sorted_rank[i][1]) + "\n"
        outf.write(res)
