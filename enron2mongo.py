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
import gridfs

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )

xmlSource = sys.argv[1]
xmlFile = open(xmlSource)
xmldict = xmltodict.parse(xmlFile)
xmldict_root = xmldict["Root"]["Batch"]["Documents"]["Document"]
sourceDir = "/enron/edrm-enron-v2"
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
jobs = []

dictLength = len(xmldict["Root"]["Batch"]["Documents"]["Document"])

def mongoConnector():
    connection = MongoClient(target,port,replicaSet=repSet,serverSelectionTimeoutMS=5000,connectTimeoutMS=5000)
    connection.admin.authenticate(username,password)
    return connection

def pretty(messyDict):
    return json.dumps(messyDict,sort_keys=True,indent=4, separators=(',', ': '))

def generateMongoDoc(nativeDict, increment):
    mongoDoc = OrderedDict()
    mongoDoc["_id"] = nativeDict[increment]["@DocID"]
    mongoDoc["From"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#From")
    mongoDoc["To"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#To")
    mongoDoc["Date"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#DateSent")
    mongoDoc["Attachment"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#HasAttachments")
    mongoDoc["AttachmentName"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#AttachmentNames")
    mongoDoc["AttachmentCount"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#AttachmentCount")
    mongoDoc["Subject"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#Subject")
    mongoDoc["CC"] = extractNamedTag(nativeDict[increment]["Tags"]["Tag"],"#CC")
    mongoDoc["Body"] = extractEmailBody(nativeDict[increment]["Files"]["File"])

    #print pretty(mongoDoc)
    #print mongoDoc
    return mongoDoc 

def extractNamedTag(arrayOfDocs, tagName):
    for doc in arrayOfDocs:
        if doc["@TagName"] == tagName:
            return doc["@TagValue"]

def extractEmailBody(arrayOfDocs):
    for doc in arrayOfDocs:
        if doc["@FileType"] == "Text":
            emailFile = sourceDir + "/" + doc["ExternalFile"]["@FilePath"] + "/" + doc["ExternalFile"]["@FileName"]
            email_file = open(emailFile,"r")
            emailBody = email_file.read()
    return emailBody

def batchSaveToDB(requestList):
    results = col.bulk_write(requestList, ordered=True)
    
    return results

conn = mongoConnector()
db = conn[database_name]
col = db[collection_name]
grid = gridfs.GridFS(db)

print dictLength
#print pretty(xmldict["Root"]["Batch"]["Documents"]["Document"][0] )


while position != dictLength:
    if (dictLength - position)  > batchSize:
        requestList = []
        for i in xrange(batchSize):
            if xmldict_root[position].has_key('Locations') == False:
                print "No Location key found"
                position = position + 1
                continue
            if "Calendar" in xmldict_root[position]["Locations"]["Location"]["LocationURI"]:
                print "found Calendar entry"
                position = position + 1
                continue
            requestList.append(InsertOne(generateMongoDoc(xmldict_root, position))) 
            position = position + 1
        if len(requestList) > 0 :  
            results = batchSaveToDB(requestList)
            print results.bulk_api_result
            print results.acknowledged
        requestList = []
    else:
        requestList = []
        for i in xrange(dictLength - position):
            if xmldict_root[position].has_key('Locations') == False:
                print "No Location key found"
                position = position + 1
                continue
            if "Calendar" in xmldict_root[position]["Locations"]["Location"]["LocationURI"]:
                print "found Calendar entry"
                position = position + 1
                continue
            requestList.append(InsertOne(generateMongoDoc(xmldict_root, position)))
            position = position + 1
        if len(requestList) > 0 :
            results = batchSaveToDB(requestList)
            #print results
            #print requestList
            print results.bulk_api_result
            print results.acknowledged
        requestList = []
print pretty(xmldict_root[0])
'''            
position = 0
while position != dictLength:
        
   if xmldict_root[position]["DocType"] == "File" :
        filePath =  sourceDir + "/" + xmldict_root[position]["Tag"]["Tags"]
'''
