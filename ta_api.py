

import os,sys,re, csv
import datetime, time
import random
import json
from flask import Flask, render_template, json, request, redirect, jsonify, url_for, session
import requests
import numpy as np
#from simple_salesforce import Salesforce
import spacy
nlp = spacy.load('en_core_web_sm')
from textblob import TextBlob
#import math
import pickle


################################################################################################
#
#   Flask App
#
################################################################################################


app = Flask(__name__)
app.secret_key = os.urandom(24)


################################################################################################
#
#   Functions
#
################################################################################################


def write_to_disk(object_to_write, filepathname):
    try:
        file = open(filepathname,'wb')
        file.write(json.dumps(object_to_write))
        file.close()
        print('[ INFO ] JSON object written to ' + str(filepathname))
    except:
        print('[ ERROR ] Issue writing object to disk. Check object name and file path/name')


def read_from_disk(filepathname):
    try:
        file = open(filepathname,'rb')
        print('[ INFO ] JSON object read from ' + str(filepathname))
        return json.loads(file.read())
    except:
        print('[ ERROR ] Issue reading object to disk. Check object name and file path/name')


def write_object_disk(objectname, filepathname):
    with open(filepathname, 'wb') as f:
        pickle.dump(objectname, f)
    print("[ INFO ] " + str(objectname) + " written to " + os.getcwd() + '/' + str(filepathname))


def read_object_disk(filepathname):
    with open(filepathname, 'rb') as f:
        objectname = pickle.load(f)
    print("[ INFO ] " + str(objectname) + " loaded as " + os.getcwd() + '/' + str(filepathname))
    return objectname


def cleanup_utf8_chars(input_string):
    try:
        return re.sub('(\r|\n|\t)',' ',re.sub(r'[^\x00-\x7F]+',' ', input_string)).strip()
    except:
        return input_string


def handle_encoding(text):
    return "".join(i for i in text if ord(i)<128)


def count_categories(text, term_list):
    return len(re.findall('('+'|'.join(term_list)+')',text,re.IGNORECASE))


def dedup(list):
    seen = set()
    out  = []
    for i in list:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def extract_dates(text):
    '''
    Detect and extract dates, times, timelines, etc and the associated sentence/statement.
    text = 'I ran a model two years ago. It was still running on January 15, 2018.'
    '''
    text = cleanup_utf8_chars(text)
    #text = cleanup_utf8_chars(text).decode('utf-8')
    doc  = nlp(text)
    #dates = re.findall('([0-9]+\/[0-9]+\/[0-9]+])', text)
    matches = {"results": [{"match":ent.text, "sentence":str(doc[ent.start : ent.end].sent)} for ent in doc.ents if ent.label_=='DATE']}
    return matches


def extract_currency(text):
    '''
    *** IN PROGRESS ***
    Detect and extract currency and associated sentence/statement.
    text = 'this is a $50 product. This second one is only 5 dollars. The last one is about thirty thousand dollars.'
    '''
    text = cleanup_utf8_chars(text)
    #text = cleanup_utf8_chars(text).decode('utf-8')
    doc  = nlp(text)
    #matches = {"results": [{"match":ent.text, "sentence":str(doc[ent.start : ent.end].sent)} for ent in doc.ents if ent.label_=='MONEY']}
    #currencies = re.findall('\$[0-9\,\. kM]+',text)
    pattern     = r'(\$[0-9\,\.]+[kM]|\$[0-9\,\.]+|\$|budget|dollar)'
    raw_matches = dedup([str(doc[token.i-1 : token.i+1].sent) for token in doc if re.search(pattern,token.text.lower())])
    matches     = {"results": [{"sentence": match } for match in raw_matches]}
    return matches


def extract_question(text):
    '''
    *** IN PROGRESS ***
    Extract questions
    text = 'where is the document that i sent last thursday. this is not a question. how can i buy nifi. this is right?'
    '''
    text = cleanup_utf8_chars(text)
    #text = cleanup_utf8_chars(text).decode('utf-8')
    doc  = nlp(text)
    pattern     = r'(where can|where is|where does|what is|what can|what will|what does|when is|when can|when will|how does|how will|how can|why is|why are|\?)'
    
    raw_matches = []
    for k,token in enumerate(doc):
        if k<len(doc)-1:
            if re.search(pattern, str(doc[k].text.lower())+' '+str(doc[k+1].text.lower()) ):
                raw_matches.append( str(doc[token.i : token.i+1].sent) )
    
    matches = {"results": [{"sentence": match } for match in dedup(raw_matches) ]}
    return matches


def extract_followups(text):
    '''
    *** IN PROGRESS ***
    Extract follow-up comments
    text = "I'll follow-up on that. This is a normal statement. This is something else that I'll get back to you on. Let's touchbase next Monday."
    '''
    text = cleanup_utf8_chars(text)
    #text = cleanup_utf8_chars(text).decode('utf-8')
    doc  = nlp(text)
    pattern = r'(get back to|follow-up|follow up)'
    matches = {"results": [{"match":token.text, "sentence": str(doc[token.i : token.i+1].sent) } for token in doc if re.search(pattern,token.text.lower())]}
    return matches


def product_category(text, number_of_results=5):
    text = text
    #text = text.decode('utf-8')
    category_map =  {
                        "nifi":['nifi'],
                        "minifi":['minifi'],
                        "storm":['storm'],
                        "kafka":['kafka'],
                        "streaming analytics manager":['streaming analytic',' sam ','stream processing','real-time','realtime'],
                        "schema registry":['schema registry','schemaregistry'],

                        "ambari":['amabari','operations'],
                        "ranger":['ranger','security'],
                        "knox":['knox','proxy','gateway'],
                        "atlas":['atlas','governance','lineage'],
                        "zookeeper":['zookeeper'],

                        "hdfs":['hdfs'],
                        "yarn":['yarn','resource manager','resourcemanager'],
                        "mapreduce":['mapreduce'],
                        "hive":['hive','sql',' tez ','llap'],
                        "hbase":['hbase','phoenix','region server','region master'],
                        "sqoop":['sqoop','bulkload'],
                        "oozie":['oozie'],
                        "spark":['spark','data science','machine learning','deep learning','analytic'],
                        "zeppelin":['zeppelin','data science','machine learning','analytic','notebook','code editor'],
                        "druid":['druid','olap'],
                        "solr":['solr','search and indexing','indexing','search'],

                        "smartsense":['smartsense','smart sense'],

                        "metron":['metron','cybersecurity'],

                        "cloudbreak":['cloudbreak'],

                        "data lifecycle manager":['data lifecyle manager','dataplane','dlm'],
                        "data steward studio":['data steward studio','dataplane','dss'],
                        "data analytics studio":['data analytics studio','dataplane','das'],

                        "data science experience":['data science experience','dsx','data science','machine learning','rstudio','jupyter','model management','model deployment'],
                        "bigsql":['bigsql','big sql','ansi sql','edw offload','edw migration'],
                        
                        "professional services":['professional service',' ps '],

                        "techinical support":['support','subscription']
                    }
    
    category_matches = {}
    values = []
    for k,v in category_map.items():
        value = count_categories(text, v)
        category_matches[k] = value
        values.append(value)
    
    if max(values) != 0:
        category_matches_standardized = [(k,v/float(max(values))*100) for k,v in category_matches.items()]
    else:
        category_matches_standardized = [(k,0) for k,v in category_matches.items()]
    
    return {"results": [i for i in sorted(category_matches_standardized, key=lambda item: item[1], reverse=True) if i[1]>0][:number_of_results] }


def detect_doc_sentiment(text):
    text = text
    #text = text.decode('utf-8')
    polarity = TextBlob(text).sentiment
    return {"sentiment":polarity[0], "subjectivity":polarity[1]}


def extract_negative_sentiment_phrases(text):
    '''
    Sentiment Range: -1 to 1 (negative to positive)
    text = "i love nifi and am really happy with hive, but am not happy with impala"
    '''
    text = cleanup_utf8_chars(text)
    #text = cleanup_utf8_chars(text).decode('utf-8')
    segments = TextBlob(text).sentences
    segments = dedup(segments)
    phrases = {"results": [{"sentiment":segment.sentiment[0], "sentence":segment.string} for segment in segments if segment.sentiment[0]<-0.50] }
    return phrases


def extract_positive_sentiment_phrases(text):
    '''
    Sentiment Range: -1 to 1 (negative to positive)
    text = "i love nifi and am really happy with hive, but am not happy with impala"
    '''
    text = text
    #text = text.decode('utf-8')
    segments = TextBlob(text).sentences
    segments = dedup(segments)
    phrases = {"results": [{"sentiment":segment.sentiment[0], "sentence":segment.string} for segment in segments if segment.sentiment[0]>0.50] }
    return phrases


################################################################################################
#
#   Endpoints
#
################################################################################################


@app.route('/api/category/tech', methods = ['GET','POST'])
def api_category_tech():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"i am using nifi for deep learning and stream processing. I also need a good code editor to write my spark jobs."}' http://localhost:5555/api/category/tech
        '''
        text = request.json['text']
        return jsonify(product_category(text))


@app.route('/api/sentiment', methods = ['GET','POST'])
def api_sentiment():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"i love nifi and am really happy with hbase, but am not happy with cassandra"}' http://localhost:5555/api/sentiment
        '''
        text = request.json['text']
        return jsonify(detect_doc_sentiment(text))


@app.route('/api/sentiment/positive', methods = ['GET','POST'])
def api_sentiment_positive():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"i love nifi and am really happy with hbase, but am not happy with cassandra"}' http://localhost:5555/api/sentiment/positive
        '''
        text = request.json['text']
        return jsonify(extract_positive_sentiment_phrases(text))


@app.route('/api/sentiment/negative', methods = ['GET','POST'])
def api_sentiment_negative():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"i love nifi and am really happy with hbase, but am not happy with cassandra"}' http://localhost:5555/api/sentiment/negative
        '''
        text = request.json['text']
        return jsonify(extract_negative_sentiment_phrases(text))


@app.route('/api/dates', methods = ['GET','POST'])
def api_dates():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"I ran a model two years ago. It was still running when I checked on January 15, 2018."}' http://localhost:5555/api/dates
        '''
        text = request.json['text']
        return jsonify(extract_dates(text))


@app.route('/api/currency', methods = ['GET','POST'])
def api_currency():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"Our budget is $500k this year. This sentence does not mention any currency. We also have about fifty thousand dollars from last years budget"}' http://seregion7.field.hortonworks.com:5555/api/currency
        '''
        text = request.json['text']
        return jsonify(extract_currency(text))


@app.route('/api/questions', methods = ['GET','POST'])
def api_questions():
    if request.method == 'POST':
        '''
        curl -i -H "Content-Type: application/json" -X POST -d '{"text":"Our budget is $500k. When can you get onsite for a reference architecture. How does NiFi and MiniFi adddress my use case. this is correct?"}' http://localhost:5555/api/questions
        '''
        text = request.json['text']
        return jsonify(extract_question(text))


################################################################################################
#
#   Run App
#
################################################################################################

if __name__ == "__main__":
    app.run(threaded=False, host='0.0.0.0', port=5555)



#ZEND
