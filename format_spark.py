from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import re

# data
chunk_size = 15
PATTERN = re.compile('([A-Za-z0-9_/-]+)', re.UNICODE)

stop_words = set ('ll,don,go,ve,those,much,over,even,again,isn,a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your'.split(','))

start_date = "2000-01-01"
#end_date = "2015-05-01"
end_date =  "2015-06-01"

query = {
    "query": {
        "match_all": {

        }
    },
    "filter": {
        "range" : {
            "metadata.date" : {
                "gte" : start_date,
                "lt" : end_date
            }
        }
    },
    "sort": [
            {
            "metadata.date" : {"order" : "asc"}
        },
            {
            "metadata.link" : {"order" : "asc"}
        }
    ]
}


def unique_list(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if not (x in seen or seen_add(x))]


def filter_dictionary(dictionary_counts, min_occurences, max_occurences):
    for i in dictionary_counts.keys():
        if dictionary_counts[i]>(max_occurences) or dictionary_counts[i]<min_occurences:
            dictionary_counts.pop(i, None)

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


def convert_count_to_index(dictionary_counts):
    j=0
    index = {}
    for i in dictionary_counts.keys():
        index[i]=j
        j += 1
    return index


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


def build_dict(query):

    dictionary_counts = {}
    document_count = 0

    scan_resp = helpers.scan(client=elastic_search, query=query, scroll="10m", timeout="10m", preserve_order=True)
    for resp in scan_resp:

        # merge dialog
        all_dialogs = map(lambda x : x['dialog']+" ",resp['_source']['content'])


        if len(all_dialogs)>0:
            # break into document chunks
            chunks = build_chunks(all_dialogs)

            # iterate over document chunks
            for chunk in chunks:
                tokens = tokenize(chunk)
                for word in unique_list(tokens):
                    if word not in stop_words:
                        if word in dictionary_counts:
                            dictionary_counts[word] = dictionary_counts[word] + 1
                        else:
                            dictionary_counts[word] = 1
                document_count += 1

    return dictionary_counts, document_count


def build_matrix(query, index_dict, f_index, f_matrix, f_matrix_linear, f_yahoo, f_yahoo_words):

    global_index = 0

    scan_resp = helpers.scan(client=elastic_search, query=query, scroll="10m", timeout="10m", preserve_order=True)
    for resp in scan_resp:

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

            f_index.write(str(global_index)+'\t'+str(index*chunk_size)+'\t'+str(date)+'\t'+link+'\t'+id+'\n')
            f_matrix.write(str(global_index))
            f_yahoo.write(str(global_index)+" "+str(global_index))
            f_yahoo_words.write(str(global_index)+" "+str(global_index))

            for word in tokens:
                if word in index_dict:
                    word_index = index_dict[word]
                    f_matrix.write('\t'+str(word_index))
                    f_yahoo.write(' '+str(word_index))
                    f_yahoo_words.write(' '+word)
                    f_matrix_linear.write(str(global_index)+'\t'+str(word_index)+'\n')

            f_matrix.write('\n')
            f_yahoo.write('\n')
            f_yahoo_words.write('\n')

            global_index += 1


#####MAIN#####


# initialize elastic search
elastic_search = Elasticsearch()

# parse content to build dictionary
dictionary_counts, document_count = build_dict(query)
print "size of dictionary before filtering: %i" % len(dictionary_counts)

# filter dictionary to remove extreme
filter_dictionary(dictionary_counts, 5, document_count/2)
index_dict = convert_count_to_index(dictionary_counts)
print "size of dictionary after filtering: %i" % len(dictionary_counts)

# write out dict
hash = {}
f_dict = open ('data/cnn_news_'+start_date+'_'+end_date+'_'+str(chunk_size)+'.dict', 'w')
for key in index_dict.keys():
    hash[index_dict[key]]=key
for index in range(len(hash)):
    value = hash[index].encode('ascii', 'ignore').decode('ascii')
    f_dict.write(str(index) + '\t' + value + '\t' + str(dictionary_counts[hash[index]])+'\n')
f_dict.close()


# process and save matrix
f_index = open ('data/cnn_news_'+start_date+'_'+end_date+'_'+str(chunk_size)+'.index', 'w')
f_matrix = open ('data/cnn_news_'+start_date+'_'+end_date+'_'+str(chunk_size)+'.matrix', 'w')
f_matrix_linear = open ('data/cnn_news_'+start_date+'_'+end_date+'_'+str(chunk_size)+'.linear', 'w')
f_yahoo = open ('data/cnn_news_'+start_date+'_'+end_date+'_'+str(chunk_size)+'.yahoo', 'w')
f_yahoo_words = open ('data/cnn_news_'+start_date+'_'+end_date+'_'+str(chunk_size)+'.yahoo_words', 'w')

build_matrix(query, index_dict, f_index, f_matrix, f_matrix_linear, f_yahoo, f_yahoo_words)

f_matrix.close()
f_index.close()
f_matrix_linear.close()
f_yahoo.close()
f_yahoo_words.close()
