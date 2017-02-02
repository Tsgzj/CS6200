from parser import list_all_files, extract_docs
from tokenizer import init_stopwords, tokenize
import operator
from multiprocessing import Pool, Queue
import random
import os.path
import json
from collections import deque
from b64 import e_b64

data_path = "/Users/Sun/Documents/IR/Data/HW1/ap89_collection/"
# data_path = "/Users/Sun/Documents/IR/Data/HW1/test/"
INDEX_DIR =  "/Users/Sun/Documents/IR/Data/HW2/index/"

# Up to 1000 posting
def chunk_file(files, n):
    for i in xrange(0, len(files), n):
        yield files[i:i+n]

def invert_doc(docs, stopwords):
    doc_map = [] # keep the lenth of doc and mapping
    index = {}

    for doc in docs:
        terms = tokenize(doc["text"], stopwords)
        doc_map.append([doc["docno"], len(terms)])

        for pos, t in enumerate(terms):
            if t in index:
                if doc["docno"] in index[t]:
                    index[t][doc["docno"]].append(pos)
                else:
                    index[t][doc["docno"]] = [pos]
            else:
                index[t] = {}
                index[t][doc["docno"]] = [pos]

    yield doc_map
    yield index

def sort_by_value(dic):
    return sorted(dic.items(), key=operator.itemgetter(1), reverse = True)

def sort_by_key(dic):
    return sorted(dic.items(), key=operator.itemgetter(0))

# def build_catalog(index):


s = init_stopwords()
# doc_map, index = build_invert_doc(get_file(), s)

def write_index(index, myfile):
    fi = open(myfile + "inv" , 'w')
    fc = open(myfile + "cat" , 'w')

    pos = 0
    ln = 1 # line number starts with 1
    for i in sort_by_key(index):
        term = i[0] + ' '
        fi.write(term)
        fc.write(term)
        fc.write(str(pos) + ' ' + str(ln) + '\n')
        pos += len(term)
        # for b in sort_by_value(i[1]):
            # block = '(' + str(b[0]) + ',' + str(b[1]) + ')'
        block = json.dumps(i[1], separators=(',',':'))
        pos += len(block)
        fi.write(block)
        fi.write('\n')
        pos += 1
        ln += 1
    fi.close()
    fc.close()

def write_map(doc_map, myfile):
    f = open(myfile + "map", 'w')

    for i in doc_map:
        mapping = str(i[0]) + ' ' + str(i[1]) + '\n'
        f.write(mapping)

def random_file_name(file_dir):
    rand = random.Random()
    randi = rand.randint(1, 1000)
    out_file = file_dir + str(randi).zfill(4)
    while os.path.isfile(out_file + 'inv'):
        randi = rand.randint(1, 1000)
        out_file = file_dir + str(randi).zfill(4)
    return out_file


def build_work(file_list):
    docs = []
    for f in file_list:
        for i in extract_docs(data_path + f):
            docs.append(i)

    doc_map, ind = invert_doc(docs, s)
    out_file = random_file_name(INDEX_DIR)
    write_index(ind, out_file)
    write_map(doc_map, out_file)

def list_files_end_with(file_dir, tail):
    return [f for f in os.listdir(file_dir) if f.endswith(tail)]

def merge_map(file_dir):
    l = list_files_end_with(file_dir, 'map')
    ofile =  open(file_dir + 'tmap', 'w')
    for fname in l:
        infile = open(file_dir + fname)
        for line in infile:
            ofile.write(line)
        infile.close()
        # os.remove(file_dir + fname)
    ofile.close()
    print "Map merging finished"

    for fname in l:
        os.remove(file_dir + fname)

def read_cat(cat_file_name):
    cat = deque()
    for i in open(cat_file_name):
        a = i.split(' ')
        cat.append({a[0]: a[1]})
    return cat

def read_inv_line(inv_file, offset):
    inv_file.seek(offset)
    l = inv_file.readline().split(' ')
    return json.loads(l[1])

def merge_dblock(h1, h2):
    res = h1.copy()
    res.update(h2)
    return res

def merge_files(lfile):
    ## a list of file
    ## lenth can be 1
    if len(lfile) == 1:
        return

    print "Merging " + lfile[0] + ' ' + lfile[1]

    f1 = open(INDEX_DIR + lfile[0])
    f2 = open(INDEX_DIR + lfile[1])
    c1 = read_cat(INDEX_DIR + lfile[0].replace('inv', 'cat'))
    c2 = read_cat(INDEX_DIR + lfile[1].replace('inv', 'cat'))
    out = random_file_name(INDEX_DIR)
    fouti = open(out + 'inv', 'a')
    foutc = open(out + 'cat', 'a')
    pos = 0
    ln = 0
    while c1 and c2:
        if c1[0].keys()[0] == c2[0].keys()[0]:
            dblock = merge_dblock(read_inv_line(f1, int(c1[0].values()[0])),
                                  read_inv_line(f2, int(c2[0].values()[0])))
            wline = c1[0].keys()[0]+ ' ' + json.dumps(dblock, separators=(',',':')) + '\n'
            fouti.write(wline)
            foutc.write(c1[0].keys()[0] + ' ' + str(pos) + ' ' + str(ln) + '\n')
            pos += len(wline)
            ln += 1
            c1.popleft()
            c2.popleft()
        elif c1[0].keys()[0] < c2[0].keys()[0]:
            dblock = read_inv_line(f1, int(c1[0].values()[0]))
            wline = c1[0].keys()[0]+ ' ' + json.dumps(dblock, separators=(',',':')) + '\n'
            fouti.write(wline)
            foutc.write(c1[0].keys()[0] + ' ' + str(pos) + ' ' + str(ln) + '\n')
            pos += len(wline)
            ln += 1
            c1.popleft()
        else:
            dblock = read_inv_line(f2, int(c2[0].values()[0]))
            wline = c2[0].keys()[0]+ ' ' + json.dumps(dblock, separators=(',',':')) + '\n'
            fouti.write(wline)
            foutc.write(c2[0].keys()[0] + ' ' + str(pos) + ' ' + str(ln) + '\n')
            pos += len(wline)
            ln += 1
            c2.popleft()

    while c1:
        dblock = read_inv_line(f1, int(c1[0].values()[0]))
        wline = c1[0].keys()[0]+ ' ' + json.dumps(dblock, separators=(',',':')) + '\n'
        fouti.write(wline)
        foutc.write(c1[0].keys()[0] + ' ' + str(pos) + ' ' + str(ln) + '\n')
        pos += len(wline)
        ln += 1
        c1.popleft()

    while c2:
        dblock = read_inv_line(f2, int(c2[0].values()[0]))
        wline = c2[0].keys()[0]+ ' ' + json.dumps(dblock, separators=(',',':')) + '\n'
        fouti.write(wline)
        foutc.write(c2[0].keys()[0] + ' ' + str(pos) + ' ' + str(ln) + '\n')
        pos += len(wline)
        ln += 1
        c2.popleft()

    os.remove(INDEX_DIR + lfile[0])
    os.remove(INDEX_DIR + lfile[1])
    os.remove(INDEX_DIR + lfile[0].replace('inv', 'cat'))
    os.remove(INDEX_DIR + lfile[1].replace('inv', 'cat'))

def build_index():
    print "Building partial index..."
    p = Pool(8)
    chunk_list = list(chunk_file(list_all_files(data_path), 5))
    p.map(build_work, chunk_list)

def merge_inv():
    print "Merging index..."
    l = list_files_end_with(INDEX_DIR, 'inv')
    p = Pool(8)
    while len(l) > 1:
        chunk_list = list(chunk_file(l, 2))
        p.map(merge_files, chunk_list)
        l = list_files_end_with(INDEX_DIR, 'inv')

def clean_up():
    inv_files = list_files_end_with(INDEX_DIR, 'inv')
    if len(inv_files) > 1:
        print "Index Merge failed"
        return
    else:
        print "Index Merge succeed"

        doc_ids = {}
        ind = 0
        for i in open(INDEX_DIR + 'tmap'):
            doc_ids[i.split(' ')[0]] = e_b64(ind)
            ind += 1

        for i in inv_files:
            fi = open(INDEX_DIR + i)
            cat = read_cat(INDEX_DIR + i.replace('inv', 'cat'))

        outi = open(INDEX_DIR + 'index', 'w')
        outc = open(INDEX_DIR + 'catalog', 'w')
        ln = 0
        pos = 0
        for t in cat:
            term = t.keys()[0]
            dblock = read_inv_line(fi, int(t.values()[0]))
            mapped = {}
            for ttf in dblock:
                e_ttf = []
                for item in dblock[ttf]:
                    e_ttf.append(e_b64(item))
                mapped[doc_ids[ttf]] = e_ttf
            wline = term + ' ' + json.dumps(mapped, separators=(',',':')).replace('"', '') + '\n'
            outi.write(wline)
            outc.write(term + ' ' + str(pos) + ' ' + str(ln) + '\n')
            ln += 1
            pos += len(wline)

        tmap = open(INDEX_DIR + 'tmap')
        mapout = open(INDEX_DIR + 'map', 'w')
        for i in tmap:
            a = i.split(' ')
            mapout.write(str(doc_ids[a[0]]) + ' ' + str(int(a[1])) + '\n')


        outm = open(INDEX_DIR + 'idmap', 'w')
        for i in doc_ids:
            outm.write(doc_ids[i] + ' ' + str(i) + '\n')

        os.remove(INDEX_DIR + inv_files[0])
        os.remove(INDEX_DIR + inv_files[0].replace('inv', 'cat'))
        os.remove(INDEX_DIR + 'tmap')

def merge_index():
    merge_map(INDEX_DIR)
    merge_inv()

def test():
    for i in list_files_end_with(INDEX_DIR, 'cat'):
        print read_cat(INDEX_DIR + i)[0]
        return

    for i in list_files_end_with(INDEX_DIR, 'inv'):
        print i
        f = open(INDEX_DIR + i)
        read_inv_line(f, 5395167)


if __name__ == '__main__':
    build_index()
    merge_index()
    clean_up()
