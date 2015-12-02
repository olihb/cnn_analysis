from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import re
from tqdm import *
from collections import defaultdict


# filename
dictionary_file = "data/cnn/cnn.word_id.dict"
svm_file = "data/cnn/cnn.libsvm"
linear_matrix_file = "data/cnn/cnn.linear"
date_file = "data/cnn/cnn.date"


stopword_file = "stopwords.txt"

# config
chunk_size = 15
PATTERN = re.compile('([A-Za-z0-9_/-]+)', re.UNICODE)

# config dates
start_date = "2000-01-01"
end_date =  "2000-02-01"

# data structures
stop_words = set()
dictionary_index = dict()
dictionary_counts = dict()
current_dict_index = 0

query = {
    "query": {"match_all": {}},
    "filter": {"range" : {"metadata.date" : {"gte" : start_date,"lt" : end_date}}},
    "sort": [{"metadata.date" : {"order" : "asc"}},{"metadata.link" : {"order" : "asc"}}]
}

def load_stopwords(filename):
    with open(filename, "r") as file:
        for line in tqdm(file):
            stop_words.add(line.strip())

def tokenize(text):
    output = []

    # to lower case
    text = text.lower()

    # split to token
    for match in PATTERN.finditer(text):
        item = match.group()
        if len(item)>1:
            if item[-1]=='-':
                item=item[:-1]
            output.append(item)

    return output


def build_chunks(all_dialog):
    output = []
    current_chunk = []
    for index, dialog in enumerate(all_dialog):
        if index > 0 and index % chunk_size == 0:
            output.append(" ".join(current_chunk))
            current_chunk = []
        else:
            current_chunk.append(dialog)

    # remaining dialog lines should be a new doc only if 1/2 of the chunk size, otherwise merge
    content = " ".join(current_chunk)

    if len(current_chunk)>chunk_size/2:
        output.append(content)
    else:
        if len(output)==0:
            output.append(content)
        else:
            output[-1]=output[-1]+content
    return output


def process_word(word):
    global current_dict_index
    if word in dictionary_index:
        dictionary_counts[word]+=1
        return dictionary_index[word]
    else:
        dictionary_counts[word]=1
        dictionary_index[word]=current_dict_index
        current_dict_index+=1
        return dictionary_index[word]

def build_matrix(elastic_search, query, f_svm, f_date, f_linear):

    matrix_index = 1

    scan_resp = helpers.scan(client=elastic_search, query=query, scroll="10m", timeout="10m", preserve_order=True)
    for resp in tqdm(scan_resp):

        # get metadate indicator
        date = resp['_source']['metadata']['date']
        link = resp['_source']['metadata']['link']
        id = resp['_id']
        #print date

        # merge dialog
        all_dialogs = map(lambda x : x['dialog']+" ",resp['_source']['content'])

        # break into document chunks
        chunks = build_chunks(all_dialogs)

        # iterate over document chunks
        for index, chunk in enumerate(chunks):
            tokens = tokenize(chunk)

            f_svm.write(str(matrix_index)+'\t')

            line = defaultdict(lambda: 0)

            for word in tokens:
                if word not in stop_words:
                    word_index = process_word(word)
                    line[word_index]+=1
                    f_linear.write(str(matrix_index)+'\t'+str(word_index)+'\n')

            for index, token in enumerate(line.keys()):
                if index>0:
                    f_svm.write(' ')
                f_svm.write(str(token)+':'+str(line[token]))

            f_svm.write('\n')
            f_date.write(str(matrix_index)+'\t'+date+'\t'+link+'\n')
            matrix_index += 1


def main():

    # initialize elastic search
    elastic_search = Elasticsearch()

    # parse content to build dictionary
    load_stopwords(stopword_file)

    # process and save matrix
    f_svm = open (svm_file, 'w')
    f_date = open (date_file, 'w')
    f_linear = open (linear_matrix_file, 'w')
    build_matrix(elastic_search, query, f_svm, f_date, f_linear)
    f_svm.close()
    f_date.close()
    f_linear.close()

    # write out dict
    f_dict = open (dictionary_file, 'w')
    for word in dictionary_index.keys():
        id = dictionary_index[word]
        counts = dictionary_counts[word]
        f_dict.write(str(id) + '\t' + word + '\t' + str(counts)+'\n')
    f_dict.close()

if __name__ == "__main__":
    main()