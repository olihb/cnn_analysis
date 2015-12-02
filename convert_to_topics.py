import sqlite3 as lite
import sys
import re
from tqdm import *

doc_index_file = "data/cnn_news_2000-01-01_2015-06-01_15.index"
topic_file = "data/lda.docToTop.txt"
PATTERN = re.compile('\(([0-9]*),([0-9.]*)\)', re.UNICODE)

def create_tables(cur):
    cur.execute("drop table if exists document_index")
    cur.execute("drop table if exists document_topics")
    cur.execute("create table document_index(id int, chunk id, stamp date, url text, md5 text)")
    cur.execute("create table document_topics(document_id int, topic_id int, similarity float)")
    print "tables created"

def load_doc_index(cur, filename):
    print "load document index"
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file):
            lst.append(line.split("\t"))
    cur.executemany("insert into document_index values (?,?,?,?,?)",lst)

def load_topic_file(cur, filename):
    print "load topic assignation"
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file):
            cells = line.strip().split("\t")
            doc_id = cells[0]
            topics = cells[2].split(" ")
            for topic in topics:
                for match in PATTERN.finditer(topic):
                    item = match.groups()
                    topic_id = item[0]
                    similarity = item[1]
                    lst.append((doc_id,topic_id, similarity))
            if len(lst)>50000:
                cur.executemany("insert into document_topics values (?,?,?)", lst)
                lst = list()
    cur.executemany("insert into document_topics values (?,?,?)", lst)

def main():

    con = None
    try:
        con = lite.connect('data/cnn/topics.db')
        cur = con.cursor()

        create_tables(cur)
        con.commit()

        load_doc_index(cur, doc_index_file)
        con.commit()

        load_topic_file(cur, topic_file)
        con.commit()

    except lite.Error, e:
        print "Error %s:" % e.args[0]
        sys.exit(1)
    finally:
        if con:
            con.close()


if __name__ == "__main__":
    main()


