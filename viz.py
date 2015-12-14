from collections import defaultdict
import getopt
import sqlite3 as lite
from scipy import sparse
from sklearn.manifold.t_sne import TSNE
from tqdm import *
import numpy as np
import sys
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure, show

from sklearn import datasets
from sklearn.decomposition import PCA
import sklearn
from sklearn.preprocessing import scale

# database
database = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/topics.db"


output_file_csv = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/matrix_topic.csv"
output_file_csv_dict = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/matrix_topic_dict.csv"

def load_data_structures(cur, config_name):

    # words dictionary
    words = dict()

    cur.execute("select word_index, word from word_matrix_words where id = ?",(config_name,))
    rows = cur.fetchall()
    for row in tqdm(rows, leave=True):
        words[int(row[0])]=row[1]

    # create matrix
    row_list = list()
    col_list = list()
    data_list = list()
    max_tuples = dict()


    cur.execute("select word_index, topic_id, similarity from word_matrix where id = ?",(config_name,))
    rows = cur.fetchall()
    for cell in tqdm(rows, leave=True):
        row = int(cell[0])
        col = int(cell[1])
        data = float(cell[2])

        if row in max_tuples:
            if max_tuples[row][1]<data:
                max_tuples[row]=(col, data)
        else:
            max_tuples[row]=(col, data)

        row_list.append(row)
        col_list.append(col)
        data_list.append(data)

    topics = list()
    for x in range(len(max_tuples)):
        topics.append(max_tuples[x][0])

    mrow = np.array(row_list)
    mcol = np.array(col_list)
    mdata = np.array(data_list)
    mtx = sparse.csr_matrix((mdata, (mrow, mcol)))

    return words, mtx, topics

def save_transformation(cur, name, algo, matrix, topics):
    lst = list()
    for i in tqdm(range(len(topics))):
        x = matrix[i,0]
        y = matrix[i,1]
        c = topics[i]
        lst.append((name,algo,i,x,y,c))
    cur.executemany("insert into computed_viz values (?,?,?,?,?,?)", lst)

def load_and_transform(con, cur, tag):
    # erase old data
    cur.execute("delete from computed_viz where id = ?",(tag,))
    con.commit()

    # load/transform data structure
    words, mtx, topics = load_data_structures(cur, tag)

    # compute
    matrix = mtx.toarray()

    # PCA
    pca = PCA(n_components=2)
    X_r = pca.fit(matrix).transform(matrix)
    save_transformation(cur, tag, "pca", X_r, topics)
    con.commit()

    # T-SNE
    t_sne = TSNE(n_components=2, random_state=0, verbose=1)
    X_r = t_sne.fit_transform(matrix)
    save_transformation(cur, tag, "tsne", X_r, topics)
    con.commit()

def create_animation(con, cur, tag):
    sql = """select w.word, w.word_id, v.x, v.y, v.topic, m.similarity, sum(nb) n
  from word_matrix m
          join word_matrix_words w on m.word_index=w.word_index
          join computed_viz v on v.word_index=w.word_index and v.topic=m.topic_id
          join words_stats ws on ws.word_id=w.word_id
  where  algo='tsne' and ws.stamp='2001-09-11'
  group by w.word, w.word_id, v.x, v.y, v.topic, m.similarity"""



def main(argv):

    con = None
    config_name = "topic_0.25_1000"

    try:
        con = lite.connect(database)
        cur = con.cursor()
        # arguments
        try:
            opts, args = getopt.getopt(argv, "la")
        except getopt.GetoptError:
            sys.exit(2)

        for opt, arg in opts:
            # load tables
            if opt == '-l':
                load_and_transform(con, cur, config_name)

            # send to dynamodb
            elif opt =='-a':
                create_animation(con, cur, config_name)


            #def onpick3(event):
        #    ind = event.ind
        #    print "-----"
        #    for i in ind:

        #        print words[i]+" : "+str(topics[i])

        #fig = figure()
        #ax1 = fig.add_subplot(111)
        #col = ax1.scatter(X_r[:, 0], X_r[:, 1], c=topics, picker=True)
        #fig.canvas.mpl_connect('pick_event', onpick3)
        #show()

    except lite.Error, e:
        print "Error %s:" % e.args[0]
        sys.exit(1)
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    main(sys.argv[1:])
