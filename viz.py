from collections import defaultdict
import pickle
import getopt
import sqlite3 as lite
import itertools
from operator import itemgetter
from matplotlib import animation
import matplotlib

import math
import random
import collections

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

import label_position

from bokeh.plotting import figure, output_file, show, ColumnDataSource
from bokeh import palettes
from bokeh.models import HoverTool

# database
database = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/topics.db"

output_animation_prefix = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/animation/test_"
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

def create_animation(con, cur, tag, algo='tsne'):
    sql = """   select strftime('%Y-%m',ws.stamp) fdate, w.word, w.word_id, v.x, v.y, v.topic, m.similarity, sum(nb) n
                from word_matrix m
                join word_matrix_words w on m.word_index=w.word_index
                join computed_viz v on v.word_index=w.word_index and v.topic=m.topic_id
                join words_stats ws on ws.word_id=w.word_id
                where algo='tsne' and (ws.stamp between '2001-01-01' and '2015-10-01') and v.topic not in (0,21,38,84,93,99)
                group by fdate, w.word, w.word_id, v.x, v.y, v.topic, m.similarity
                having sum(nb)>0
                order by fdate"""


    # setup chart
    top_nb_label = 50
    x_size = 15
    fig = plt.figure(figsize=[x_size,x_size*4/5])
    label_position.set_renderer(fig)
    scatter = plt.scatter([],[],c=[],lw = 0)
    plt.xlim([-15,15])
    plt.ylim([-15,15])
    label=[]
    for t in range(top_nb_label):
        label.append(plt.text(.5, .5, '', fontsize=12, multialignment='center'))


    # get data
    data = {}
    list_keys = []
    #cur.execute(sql)
    #rows = cur.fetchall()
    #data_by_date = itertools.groupby(rows, key=itemgetter(0))
    #for key, items in data_by_date:
    #    data[key]=list(items)
    #    list_keys.append(key)


    #pickle.dump(data, open('data-a.p','wb'))
    #pickle.dump(list_keys, open('list-a.p', 'wb'))
    data = pickle.load(open('data-y.p','rb'))
    list_keys = pickle.load(open('list-y.p','rb'))

    def init():
        return scatter

    def update_chart(i, chart):
        key = list_keys[i]
        current_data = data[key]

        xy = map(lambda x: [x[3],x[4]], current_data)
        c_list = map(lambda x: x[5], current_data)

        a = 0.225
        max_s = max(map(lambda x: x[6]*math.log10(x[7]), current_data))
        w_list = map(lambda x: [x[1],x[3],x[4],x[6]*math.log10(x[7])], current_data)
        s_list = map(lambda x: math.pow((1.0-math.pow((x[6]*math.log10(x[7]))/max_s,a)),1.0/a)*800.0, current_data)
        w_list.sort(key=lambda x: x[3],reverse=True)

        label_position.set_positions(w_list, top_nb_label, label, 0.5)

        chart.set_array(np.array(c_list))
        chart.set_offsets(xy)
        chart.set_sizes(np.array(s_list))
        plt.title(key)
        print i
        return chart,

    anim = animation.FuncAnimation(fig, update_chart, init_func=init, frames=len(list_keys), fargs=(scatter,))
    anim.save('chart.gif',  writer='imagemagick', fps=1)
    #plt.show()

def create_chart_scatter_bokeh(con, cur, tag, algo='tsne'):

    data = pickle.load(open('data-h.p','rb'))
    list_keys = pickle.load(open('list-h.p','rb'))

    key = list_keys[0]
    current_data = data[key]

    max_s = max(map(lambda x: x[6]*math.log10(x[7]), current_data))
    a = 0.5

    source = ColumnDataSource (
        data = dict(
            x= map(lambda c: c[3], current_data),
            y= map(lambda c: c[4], current_data),
            prob = map(lambda c: int(c[6]*100), current_data),
            topic= map(lambda c: c[5], current_data),
            desc= map(lambda c: c[1], current_data),
            color = map(lambda x: palettes.Spectral11[x[5]%11], current_data),
            s=map(lambda x: math.pow((1.0-math.pow((x[6]*math.log10(x[7]))/max_s,a)),1.0/a)*0.3, current_data)
        )
    )

    hover = HoverTool(
        tooltips="""
        <div>
            <span style="font-size: 15px; font-weight: bold;">@desc</span>
            <span style="font-size: 12px; color: #966;">[@topic - @prob%]</span>
        </div>
        """
    )

    # output to static HTML file
    output_file("scatter.html",)

    # create a new plot with a title and axis labels

    p = figure()
    p.add_tools(hover)

    # add a line renderer with legend and line thickness
    p.circle('x','y', source=source, radius='s', color='color', fill_alpha=0.4)

    # show the results
    show(p)

def create_chart_heatmap_bokeh(con, cur, tag, algo='tsne'):
#    sql = """   select strftime('%Y',ws.stamp) date, v.topic, sum(nb) n
#                from word_matrix m
#                join word_matrix_words w on m.word_index=w.word_index
#                join computed_viz v on v.word_index=w.word_index and v.topic=m.topic_id
#                join words_stats ws on ws.word_id=w.word_id
#                where algo='tsne' and (ws.stamp between '2000-01-01' and '2016-01-01') and m.similarity>0.7
#                group by date, v.topic
#                having sum(nb)>1000
#                order by date, v.topic"""""
#
#    cur.execute(sql)
#    rows = cur.fetchall()
#    allo = []
#    for rr in rows:
#        allo.append(rr)
#
#
#
#    pickle.dump(allo, open('data-allo.p','wb'))
    raw_data = pickle.load(open('data-allo.p','rb'))





    fdate = []
    fdates = set()
    topic = []
    topics = set()
    data = []
    color = []
    max_topic = defaultdict(float)
    max = 0

    for row in raw_data:
        fdate.append(row[0])
        fdates.add(row[0])
        topic.append(row[1])
        topics.add(row[1])
        data.append(float(row[2]))
        if row[2]>max_topic[row[1]]:
            max_topic[row[1]]=row[2]
        if row[2]>max:
            max=row[2]

    for i in range(len(topic)):
        t = topic[i]
        data[i] = data[i]/max_topic[t]
        #data[i] = data[i]/max

        color_index=(int((data[i]*10-0.0000001))%10)
        print data[i]*10
        print color_index
        print '=---'

        color.append(palettes.RdBu10[color_index])



    source = ColumnDataSource(
        data=dict(fdate=fdate,topic=topic,data=data, color=color)
    )

    output_file("heatmap.html",)

    p = figure(x_range=[str(x) for x in list(topics)], y_range=['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015'])
    p.rect("topic","fdate", 1, 1, source=source,color='color')

    hover = HoverTool(
        tooltips=[
            ("date", "@fdate"),
            ("topic", "@topic"),
            ("data", "@data"),
        ]
    )

    p.add_tools(hover)

    show(p)




def main(argv):

    con = None
    config_name = "topic_0.25_1000"

    try:
        con = lite.connect(database)
        cur = con.cursor()
        # arguments
        try:
            opts, args = getopt.getopt(argv, "lach")
        except getopt.GetoptError:
            sys.exit(2)

        for opt, arg in opts:
            # load tables
            if opt == '-l':
                load_and_transform(con, cur, config_name)

            # send to dynamodb
            elif opt =='-a':
                create_animation(con, cur, config_name)

            elif opt == '-c':
                create_chart_scatter_bokeh(con, cur, config_name)

            elif opt == '-h':
                create_chart_heatmap_bokeh(con, cur, config_name)

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
