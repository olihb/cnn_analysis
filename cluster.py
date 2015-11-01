
import logging
from gensim import corpora, models
import pickle

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)

dictionary = corpora.Dictionary.load('data/cnn-news-15.dict')
corpus = corpora.MmCorpus('data/cnn-news-15.mm')

#model = models.DtmModel('/home/olihb/Documents/dtm_release/dtm/main', corpus, time_slices, num_topics=30, id2word=dictionary,initialize_lda=True)

#model.save("dtm.model")
#model.print_topics(20)

#print corpus

#lda = models.LdaMulticore(corpus=corpus, id2word=dictionary, num_topics=250, passes=20, batch=True)

#lda = models.LdaMulticore(corpus=corpus, id2word=dictionary, num_topics=500, iterations=250)

#lda.save("lda-cnn-250-batch.model")
lda = models.LdaMulticore.load('data/lda-cnn-250.model')

lda.print_topics(250)
