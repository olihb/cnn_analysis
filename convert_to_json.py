from collections import defaultdict
from boto.dynamodb2.table import Table
from tqdm import *
import sys
import getopt
import sqlite3 as lite
from shove import Shove

# filename
dictionary_file = 'data/cnn/cnn.word_id.dict'
date_file = 'data/cnn/cnn.date'
linear_matrix_file = 'data/cnn/cnn.linear'

# database
database = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/topics.db"


# dynamo table
dynamoDB_table = 'cnn_2015'

def load_dict(filename):
    print "Load dictionary"
    keywords = {}
    with open(filename) as file:
        for line in tqdm(file, leave=True):
            cells = line.strip().split('\t')
            keywords[cells[0]] = cells[1]
    return keywords

def load_index(filename):
    print "Load index"
    index = {}

    with open(filename) as file:
        for line in tqdm(file, leave=True):
            cells = line.strip().split('\t')
            index[cells[0]] = cells[1]
    return index

def process_index(filename, index, keywords):
    print "Process index"
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

# assumption that documents_id are grouped
def process_index_doc(filename, index):
    print "Process index for documents"
    words_visited = set()
    stamp_words = defaultdict(lambda: defaultdict(lambda: 0))
    with open(filename) as file:
        previous_line_doc_id = ""
        for line in tqdm(file, leave=True):
            cells = line.strip().split('\t')
            doc_id = cells[0]
            word_id = cells[1]
            year = index[doc_id]
            if previous_line_doc_id != doc_id:
                words_visited = set()
            if word_id not in words_visited:
                stamp_words[year][word_id] += 1
                words_visited.add(word_id)
            previous_line_doc_id = word_id

    return stamp_words

def main(argv):

    # load and transform data
    keywords = load_dict(dictionary_file)
    index = load_index(date_file)

    try:
        opts, args = getopt.getopt(argv, "td")
    except getopt.GetoptError:
        sys.exit(2)

    for opt, arg in opts:

        # load tables
        if opt == '-t':
            words_date, occurrences_by_words, occurrences_by_date = process_index(linear_matrix_file, index, keywords)
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
                    output['source'] = linear_matrix_file
                    output['occurrences'] = list(occurrences)
                    output['dates_size'] = list(dates_size)
                    output['dates'] = list(dates)
                    output['occurrences_size'] = occurrences_by_words[word]

                    batch.put_item(data=output)

        # send to database
        elif opt =='-d':

            con = lite.connect(database)
            cur = con.cursor()

            # write correspondence to db
            cur.execute("drop table if exists words_stats")
            # create tables
            cur.execute("create table words_stats(stamp date, word_id int, nb int)")

            # export to db
            lst = list()
            words_stats = process_index_doc(linear_matrix_file,index)
            for date in tqdm(words_stats.keys(), leave=True):
                stats = words_stats[date]
                for word_id in stats.keys():
                    nb_doc = stats[word_id]
                    if nb_doc>10: # otherwise, too big and doesn't bring a lot of information
                        lst.append((date,int(word_id),nb_doc))
                if len(lst)>50000:
                    cur.executemany("insert into words_stats values (?,?,?)", lst)
                    lst = list()
            cur.executemany("insert into words_stats values (?,?,?)", lst)
            con.commit()

if __name__ == "__main__":
    main(sys.argv[1:])