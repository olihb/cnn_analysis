import sqlite3 as lite
import sys
from tqdm import *

# files
dictionary_file = "data/test/nytimes.word_id.dict"
model_file = "data/test/server_0_table_0.model"
doc_file = "data/test/doc_topic.0"

# database
database = "data/test/topics.db"

def initialize_tables(cur):
    # drop tables
    cur.execute("drop table if exists dictionary")
    cur.execute("drop table if exists model")
    cur.execute("drop table if exists document")

    # create tables
    cur.execute("create table dictionary(id int, word text, occ int)")
    cur.execute("create table model(word_id int, topic_id int, occ int)")
    cur.execute("create table document(id int, topic int, occ int)")

def load_dictionary(cur, filename):
    print "load dictionary"
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file):
            lst.append(line.split("\t"))
    cur.executemany("insert into dictionary values (?,?,?)", lst)

def load_model(cur, filename, table):
    print "load table " + table
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file):
            cells = line.split(" ")
            line_id = cells[0]
            items = cells[1:]
            for item in items:
                subitem = item.split(":")
                if len(subitem)==2:
                    lst.append((line_id, subitem[0], subitem[1]))
            if len(lst)>50000:
                cur.executemany("insert into "+table+" values (?,?,?)", lst)
                lst = list()

    cur.executemany("insert into "+table+" values (?,?,?)", lst)


def main():

    con = None
    try:
        con = lite.connect(database)
        cur = con.cursor()

        initialize_tables(cur)
        con.commit()

        load_dictionary(cur, dictionary_file)

        load_model(cur, model_file, "model")
        con.commit()

        load_model(cur, doc_file, "document")
        con.commit()

    except lite.Error, e:
        print "Error %s:" % e.args[0]
        sys.exit(1)
    finally:
        if con:
            con.close()


if __name__ == "__main__":
    main()
