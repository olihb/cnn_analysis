from collections import defaultdict
from boto.dynamodb2.table import Table
from tqdm import *

# filename
dictionary_file = 'data/cnn/cnn.word_id.dict'
date_file = 'data/cnn/cnn.date'
linear_matrix_file = 'data/cnn/cnn.linear'

# dynamo table
dynamoDB_table = 'cnn_2001'

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
            index[cells[0]] = cells[1]
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


def main():
    # read dictionary
    keywords = load_dict(dictionary_file)

    # read index
    index = load_index(date_file)

    # process matrix
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

if __name__ == "__main__":
    main()