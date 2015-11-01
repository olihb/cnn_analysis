import pickle
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
from gensim import corpora
import gensim
import datetime

__author__ = 'olihb'

# data
stop_words = set ('a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your'.split(','))

start_date = "2000-01-01"
end_date = "2015-05-01"

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


# setup elastic search
elastic_search = Elasticsearch()

# iterate over elastic search response data structure
def iter_docs(es_query, stoplist):
    scan_resp = helpers.scan(client=elastic_search, query=es_query, scroll="10m", timeout="10m", preserve_order=True)
    for resp in scan_resp:
        date = resp['_source']["metadata"]['date']
        print date
        all_dialogs = map(lambda x : x['dialog']+" ",resp['_source']['content'])
        i=0
        document = ' '
        for dialog in all_dialogs:
            if i>0 and i%15==0:
                yield (x for x in
                    gensim.utils.tokenize(document, lowercase=True, deacc=True,errors="ignore")
                    if x not in stoplist)
                document = ''
            document = document+" "+dialog
            i+=1


# setup corpus class
class ESCorpus(object):
    def __init__(self, es_query, stoplist):
        self.dictionary = corpora.Dictionary(iter_docs(es_query, stoplist))
        self.stoplist = stoplist
        self.es_query = es_query

    def __iter__(self):
        for tokens in iter_docs(self.es_query, self.stoplist):
            yield self.dictionary.doc2bow(tokens)





# iterate search results to create corpus
corpus = ESCorpus(query, stop_words)
print corpus.dictionary
corpus.dictionary.filter_extremes()
corpus.dictionary.compactify()
print corpus.dictionary
corpus.dictionary.save("data/cnn-news-15.dict")
gensim.corpora.MmCorpus.serialize("data/cnn-news-15.mm", corpus)
