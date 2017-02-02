from stemming.porter import stem
import re
## elastic uses porter as default stemmer

## Use stoplist to filter query before actually search elastic
word_list = open("/Users/Sun/Documents/IR/Data/HW1/stoplist.txt", "r")

def init_stopwords():
    stopwords = set()
    for line in word_list:
        for w in line.split():
            if w.lower() not in stopwords:
                stopwords.add(w.lower())
            # load stoplist.txt
    return stopwords

def tokenize(doc, stopwords):
    reg = re.compile(r'(\w+(?:\.?\w+)*)')
    return map(lambda t: stem(t),
               filter(lambda t: t not in stopwords, reg.findall(doc.lower())))

def analyze_term(term, stopwords):
    if term in stopwords:
        return None
    else:
        return stem(term.lower())

## a simple test
## tokens("It was the year of Our Lord one thousand seven hundred and seventy-five. Spiritual revelations were conceded to England at that favoured period, as at this. Mrs. Southcott had recently attained her five-and-twentieth blessed birthday, of whom a prophetic private in the Life Guards had heralded the sublime appearance by announcing that arrangements were made for the swallowing up of London and Westminster. Even the Cock-lane ghost had been laid only a round dozen of years, after rapping out its messages, as the spirits of this very year last past (supernaturally deficient in originality) rapped out theirs. Mere messages in the earthly order of events had lately come to the English Crown and People, from a congress of British subjects in America: which, strange to relate, have proved more important to the human race than any communications yet received through any of the chickens of the Cock-lane brood U.S")
