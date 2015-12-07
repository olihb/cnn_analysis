from collections import defaultdict
from scipy import sparse
from sklearn.manifold.t_sne import TSNE
from tqdm import *
import numpy as np
import sys
import matplotlib.pyplot as plt

from sklearn import datasets
from sklearn.decomposition import PCA
import sklearn
from sklearn.preprocessing import scale



output_file_csv = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/matrix_topic.csv"
output_file_csv_dict = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/matrix_topic_dict.csv"

def load_data_structures(csv_file, csv_dict_file):

    # words dictionary
    words = dict()
    with open(csv_dict_file, "r") as file:
        next(file)
        for line in tqdm(file, leave=True):
            cells = line.strip().split(',')
            words[int(cells[0])]=cells[1]


    # create matrix
    row_list = list()
    col_list = list()
    data_list = list()

    max_tuples = dict()

    with open(csv_file, "r") as file:
        next(file)
        for line in tqdm(file, leave=True):
            cells = line.split(',')
            row = int(cells[0])
            col = int(cells[1])
            data = float(cells[2])

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


    #for x in range(len(topics)):
    #    print words[x]+" : "+str(topics[x])

    mrow = np.array(row_list)
    mcol = np.array(col_list)
    mdata = np.array(data_list)
    mtx = sparse.csr_matrix((mdata, (mrow, mcol)))

    return words, mtx, topics


def main(argv):
    words, mtx, topics = load_data_structures(output_file_csv, output_file_csv_dict)
    print mtx.get_shape()
    matrix = mtx.toarray()
    #matrix = sklearn.preprocessing.binarize(matrix)

    # PCA
    #pca = PCA(n_components=2)
    #X_r = pca.fit(matrix).transform(matrix)
    #plt.scatter(X_r[:, 0], X_r[:, 1], c=topics)
    #plt.show()

    model = TSNE(n_components=2, random_state=0, verbose=1)
    X_r = model.fit_transform(matrix)
    plt.scatter(X_r[:, 0], X_r[:, 1], c=topics)
    for i in range(len(topics)):
        plt.text(X_r[i,0],X_r[i,1],s=words[i],fontdict={'size':9})

    plt.show()


if __name__ == "__main__":
    main(sys.argv[1:])
