from index import Index

i = Index()

f = open("/Users/Sun/Documents/IR/Data/HW2/in.1.txt")
out = open("/Users/Sun/Dropbox/CS6200_W_Sun/HW2/result/out.txt", "a")

for t in f:
    term = t.strip()
    res = i.search(term)
    count = 0
    for x in res:
        count += int(len(res[x]))

    out.write(term + ' ' + str(len(res)) + ' ' + str(count) + '\n')
