from collections import defaultdict
import sqlite3 as lite
import sys
from tqdm import *
import getopt
from boto.dynamodb2.table import Table
import time
import json

# files
dictionary_file = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/cnn.word_id.dict"
model_file = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/server_0_table_0.model"
doc_file = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/doc_topic.0"
date_file = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/cnn.date"

# json output file
output_topic_description_file = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/topics.json"

# database
database = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/topics.db"

output_file_csv = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/matrix_topic.csv"
output_file_csv_dict = "/home/olihb/IdeaProjects/cnn_analysis/data/cnn/data/out-100/matrix_topic_dict.csv"

def initialize_tables(cur):
    # drop tables
    cur.execute("drop table if exists dictionary")
    cur.execute("drop table if exists model")
    cur.execute("drop table if exists document")
    cur.execute("drop table if exists dates")
    cur.execute("drop table if exists word_matrix")
    cur.execute("drop table if exists word_matrix_words")
    cur.execute("drop table if exists computed_viz")

    # create tables
    cur.execute("create table dictionary(id int, word text, occ int)")
    cur.execute("create table model(word_id int, topic_id int, occ int)")
    cur.execute("create table document(id int, topic int, occ int)")
    cur.execute("create table dates(id int, stamp date, url text)")
    cur.execute("create table word_matrix(id text, word_index int, topic_id int, similarity real)")
    cur.execute("create table word_matrix_words(id text, word_index int, word_id int, word text)")
    cur.execute("create table computed_viz(id text, algo text, word_id, x real, y real, topic int)")

def load_dictionary(cur, filename):
    print "load dictionary"
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file, leave=True):
            lst.append(line.split("\t"))
    cur.executemany("insert into dictionary values (?,?,?)", lst)

def load_model(cur, filename, table):
    print "load table " + table
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file, leave=True):
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

def load_dates(cur, filename):
    print "load table dates"
    lst = list()
    with open(filename, "r") as file:
        for line in tqdm(file, leave=True):
            cells = line.split("\t")
            line_id = int(cells[0])-1
            stamp = cells[1]
            url = cells[1]
            lst.append((line_id, stamp, url))
            if len(lst)>50000:
                cur.executemany("insert into dates values (?,?,?)", lst)
                lst = list()
    cur.executemany("insert into dates values (?,?,?)", lst)


def dump_to_db(cur, topic_threshold, occ_threshold):
    settings_name = "topic_{topic}_{occ}".format(topic=str(topic_threshold),occ=str(occ_threshold))

    print 'querying...'
    sql_limit = """
                select m.word_id, d.word, m.topic_id, cast(m.occ as real)/cast(d.occ as real) mm
                from model m
                join dictionary d on m.word_id=d.id
                where d.id in (
                    select word_id
                    from model m
                    join dictionary d on m.word_id=d.id
                    where (cast(m.occ as real)/cast(d.occ as real))>{topic} and d.occ>{occ})
                """.format(topic=str(topic_threshold),occ=str(occ_threshold))

    cur.execute(sql_limit)
    rows = cur.fetchall()

    print "delete stale data"
    cur.execute("delete from word_matrix where id=?", (settings_name,))
    cur.execute("delete from word_matrix_words where id=?", (settings_name,))

    print 'output to db'
    words_dict = dict()
    words_id_dict = dict()
    words_dict_index = 0

    for row in tqdm(rows, leave=True):
        word_id = row[0]
        word = row[1]
        topic = row[2]
        sim = row[3]
        if word not in words_dict:
            words_dict[word]=words_dict_index
            words_dict_index+=1
            words_id_dict[word]=word_id
        index = words_dict[word]
        cur.execute("insert into word_matrix values (?,?,?,?)",(settings_name,index,topic,sim))

    print 'output to dict'
    for word in tqdm(words_dict.keys(), leave=True):
        cur.execute("insert into word_matrix_words values (?,?,?,?)",(settings_name, words_dict[word], words_id_dict[word], word))

def dump_to_dynamo(cur, dynamoDB_table, json_output_file):
    # get occurrences by date
    print "get occurrences by date"
    sql = """   select stamp, count(*)
                from dates
                group by stamp"""
    cur.execute(sql)
    total_occ = {}
    dates = cur.fetchall()
    for date in dates:
        total_occ[date[0]]=date[1]

    # get occurences by topic
    print "get occurences by topic"
    sql = """   select m.topic, stamp, count(*) n
                from (
                    select id, topic, cast(max(occ) as real) occu,(select cast(sum(k.occ) as real) from document k where d.id=k.id) t
                    from document d
                    group by id
                ) m
                join dates da on m.id=da.id
                where occu/t>0.5
                group by m.topic, stamp
                order by m.topic, stamp asc"""
    cur.execute(sql)
    occurrences = cur.fetchall()
    topics = defaultdict(lambda: defaultdict(lambda:[]))


    print "iterate over result set"
    for row in tqdm(occurrences, leave=True):
        topic = row[0]
        stamp = row[1]
        occ = row[2]
        topics[topic]['occurrences'].append(int(occ))
        topics[topic]['dates'].append(str(stamp))
        topics[topic]['dates_size'].append(int(total_occ[stamp]))


    print "push to dynamodb table"
    table = Table(dynamoDB_table)
    with table.batch_write() as batch:
        for topic in tqdm(topics.keys(), leave=True):
            output = {}

            output['word'] = "key_topic_"+str(topic)
            output['source'] = 'load_db_topics'
            output['occurrences'] = topics[topic]["occurrences"]
            output['dates_size'] = topics[topic]["dates_size"]
            output['dates'] = topics[topic]["dates"]
            output['occurrences_size']=99

            batch.put_item(data=output)

    # output json description file
    print "output json description file"

    # get topics --because sqlite doesn't support row numbers
    topics = []
    sql = "select distinct topic_id from model"
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        topics.append(row[0])

    # iterate over sql to build json
    output = []
    for index, topic in tqdm(enumerate(topics)):
        sql = "select word from model m join dictionary d on d.id=m.word_id where topic_id=? order by m.occ desc limit 10"
        cur.execute(sql,(topic,))
        rows = cur.fetchall()
        words = []
        for word in rows:
            words.append(word[0])
        line = dict()
        line['id']=index
        line['name']=", ".join(words)
        line['key']="key_topic_"+str(topic)
        output.append(line)

    # output to file
    with open(json_output_file, 'w') as outfile:
        json.dump(output,outfile)


def main(argv):

    con = None
    try:
        con = lite.connect(database)
        cur = con.cursor()

        # arguments
        try:
            opts, args = getopt.getopt(argv, "lpc")
        except getopt.GetoptError:
            sys.exit(2)

        for opt, arg in opts:
            # load tables
            if opt == '-l':
                initialize_tables(cur)
                con.commit()

                load_dictionary(cur, dictionary_file)

                load_model(cur, model_file, "model")
                con.commit()

                load_model(cur, doc_file, "document")
                cur.execute("create index idx1 ON document(id)")
                con.commit()

                load_dates(cur, date_file)
                cur.execute("create index idx2 ON dates(id)")
                con.commit()

            # send to dynamodb
            elif opt =='-p':
                dump_to_dynamo(cur, "cnn_2015", output_topic_description_file)

            elif opt == '-c':
                dump_to_db(cur, 0.25, 1000)
                con.commit()


    except lite.Error, e:
        print "Error %s:" % e.args[0]
        sys.exit(1)
    finally:
        if con:
            con.close()


if __name__ == "__main__":
    main(sys.argv[1:])
