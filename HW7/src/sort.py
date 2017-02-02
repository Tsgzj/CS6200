import operator

def sort(infile):

    spam = {}
    ham = {}

    for line in open(infile):
        try:
            docid, label, prob = line.split()
            label = int(label)
            prob = float(prob)

            if label == 1:
                spam[docid] = prob
            else:
                ham[docid] = prob
        except:
            continue

    return spam, ham

def print_list(spam, ham, of):
    sorted_spam = sorted(spam.items(), key=operator.itemgetter(1), reverse = True)
    sorted_ham = sorted(ham.items(), key=operator.itemgetter(1), reverse = True)

    for item in sorted_spam:
        of.write(item[0] + " 1 " + str(item[1]) + "\n")

    for item in sorted_ham:
        of.write(item[0] + " 0 " + str(item[1]) + "\n")

spam, ham = sort("./../data/all_unigram.txt")
print_list(spam, ham, open("./../data/all_unigram.sorted.txt", "w"))
