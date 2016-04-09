import threading
import time
import logging
import pymongo
import sys
import timeit
import multiprocessing
import sys
import getopt
import ast
import json
import xmltodict
from collections import OrderedDict
from pymongo import MongoClient, InsertOne

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )

xmlSource = "/enron/edrm-enron-v2/zl_motley-m_763_IZGB_000.xml"
xmlFile = open(xmlSource)
xmldict = xmltodict.parse(xmlFile)
xmldict_root = xmldict["Root"]["Batch"]["Documents"]["Document"]
batchSize = 10
global retries
retries = 5
global target
target = 'myalenti-cloud-demo-0.yalenti-demo.8839.mongodbdns.com'
global port
port = 27017
global repSet
repSet = "myalenti-rpl1"
global username
username = sys.argv[2]
global password
password = sys.argv[3]
position = 0
database_name = "enron"
collection_name = "email"

dictLength = len(xmldict["Root"]["Batch"]["Documents"]["Document"])

def mongoConnector():
    connection = MongoClient(target,port,replicaSet=repSet,serverSelectionTimeoutMS=5000,connectTimeoutMS=5000)
    connection.admin.authenticate(username,password)
    return connection

def pretty(messyDict):
    return json.dumps(messyDict,sort_keys=True,indent=4, separators=(',', ': '))

def generateMongoDoc(nativeDict, increment):
    mongoDoc = OrderedDict()
    mongoDoc["DocId"] = nativeDict[increment]["@DocID"]
    #print pretty(mongoDoc)
    #print mongoDoc
    return mongoDoc

def batchSaveToDB(requestList):
    results = col.bulk_write(requestList, ordered=True)
    
    return results

conn = mongoConnector()
db = conn[database_name]
col = db[collection_name]

print dictLength
#print pretty(xmldict["Root"]["Batch"]["Documents"]["Document"][0] )


while position != dictLength:
    if (dictLength - position)  > batchSize:
        requestList = []
        for i in xrange(batchSize):
            requestList.append(InsertOne(generateMongoDoc(xmldict_root, position))) 
            position = position + 1
        #col.bulk_write(requestList, ordered=ord)
        results = batchSaveToDB(requestList)
        #print requestList
        #print " ****************"
        print results.bulk_api_result
        print results.acknowledged
        requestList = []
    else:
        requestList = []
        for i in xrange(dictLength - position):
            requestList.append(InsertOne(generateMongoDoc(xmldict_root, position)))
            position = position + 1
        results = batchSaveToDB(requestList)
        #print results
        #print requestList
        print results.bulk_api_result
        print results.acknowledged
        requestList = []
            
        




