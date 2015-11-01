from collections import defaultdict
from boto.dynamodb2.table import Table
from tqdm import *

__author__ = 'olihb@olihb.com'

#filename = 'data/cnn_news_2000-01-01_2001-01-02_15'
filename = 'data/cnn_news_2000-01-01_2015-06-01_15'
dynamoDB_table = 'cnn_2015'

def load_dict(filename):
    print "Load dictionary:"
    keywords = {}
    with open(filename) as file:
        for line in tqdm(file, leave=True):
            cells = line.strip().split('\t')
            keywords[cells[0]] = cells[1]
    return keywords

def load_index(filename):
    print "Load index:"
    index = {}

    with open(filename) as file:
        for line in tqdm(file, leave=True):
            cells = line.strip().split('\t')
            index[cells[0]] = cells[2]
    return index

def process_index(filename, index, keywords):
    print "Process index:"
    words = defaultdict(lambda: defaultdict(lambda: 0))
    occurrences = defaultdict(lambda: 0)
    occurrences_by_date = defaultdict(lambda: 0)
    with open(filename) as file:
        for line in tqdm(file, leave=True):
            cells = line.strip().split('\t')
            doc_id = cells[0]
            word_id = keywords[cells[1]]
            year = index[doc_id]
            words[word_id][year] += 1
            occurrences[word_id] += 1
            occurrences_by_date[year] += 1
    return words, occurrences, occurrences_by_date

# read dictionary
keywords = load_dict(filename+".dict")

# read index
index = load_index(filename+".index")

# process matrix
words_date, occurrences_by_words, occurrences_by_date = process_index(filename+'.linear',index, keywords)

table = Table(dynamoDB_table)
with table.batch_write() as batch:

    for word in tqdm(words_date.keys(), desc='Upload to dynamoDB', leave=True):
        output = {}
        data = words_date[word]


        occurrences = []
        dates_size = []
        dates = []

        for key,value in data.iteritems():
            occurrences.append(value)
            dates.append(key)
            dates_size.append(occurrences_by_date[key])

        zipped = zip(dates, occurrences, dates_size)
        zipped.sort()
        dates, occurrences, dates_size = zip(*zipped)

        output['word'] = word
        output['source'] = filename
        output['occurrences'] = list(occurrences)
        output['dates_size'] = list(dates_size)
        output['dates'] = list(dates)
        output['occurrences_size'] = occurrences_by_words[word]

        batch.put_item(data=output)

