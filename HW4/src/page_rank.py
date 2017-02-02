from __future__ import division

DAMPING = 0.85
THRESHOLD = 0.000000001

def page_rank(graph):
    # graph = {page: {'inlinks': [], 'outlinks': int}}

    rank = {}
    sink = {}
    N = len(graph)

    for item in graph:
        rank[item] = 1.0/N

    for item in graph:
        if graph[item]["outlinks"] == 0:
            sink[item] = {'inlinks': graph[item]['inlinks']}
    print "Sink count " + str(len(sink))

    isNotConverged = True
    count = 0
    while isNotConverged:
        print "Loop " + str(count)
        totalDiff = 0
        next_rank = {}

        sinkPR = 0
        for item in sink:
            sinkPR += rank[item]

        for item in graph:
            new_PR = (1.0 - DAMPING) / N
            new_PR += DAMPING * sinkPR / N
            for ilk in graph[item]['inlinks']:
                if rank.get(ilk) and graph.get(ilk):
                    new_PR += DAMPING * rank[ilk] / graph[ilk]['outlinks']

            next_rank[item] = new_PR

        isNotConverged = converge(graph, next_rank, rank)
        rank = next_rank
        count += 1

    return rank

def converge(graph, rank0, rank1):
    convergence = False
    for item in graph:
        convergence = convergence or (abs(rank0[item] - rank1[item]) > THRESHOLD)

    return convergence
