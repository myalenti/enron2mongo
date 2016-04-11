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

logging.basicConfig(level=logging.INFO,format='(%(threadName)4s) %(levelname)s %(message)s',)

#Maintain the source dir for use with file objects - This needs to become a parameter with getops
sourceDir = "/enron/edrm-enron-v2"

#Global Variable - Some to become getops parameters
batchSize = 10 # To become a cmd line parameter
retries = 5
target = 'myalenti-cloud-demo-0.yalenti-demo.8839.mongodbdns.com' #Needs parameterizing
port = 27017 #Needs parameterizing
repSet = "myalenti-rpl1" #Needs parameterizing
username = sys.argv[2] #Needs parameterizing
password = sys.argv[3] #Needs parameterizing
position = 0 #Position in dictionary
database_name = "enron" #Needs Parameterizing
collection_name = "email" #Needs Parameterizing


def usage():
    print "Enron database loader from xml source"
    print "     -u: username for datbase access"
    print "     -p: password for datbase access"
    print "     -t: target database replica set member"
    print "     -r: target database replica set name"
    print "     -P: target database Port"
    print "     -d: target database name"
    print "     -c: target collection name"
    print "     -s: source directory for enron source data"
    print "     --level: INFO or DEBUG"
    print "     -h: print this help output"

try:
    opts, args = getopt.getopt(sys.argv[1:], "f:u:p:t:r:P:d:c:s:", ["level=","port="])
    logging.debug("Operation list length is : %d " % len(opts))
except getopt.GetoptError:
    print "You provided invalid command line switches."
    usage()
    exit(2)

for opt, arg in opts:
    if opt in ("-u"):
        username=arg
    elif opt in ("-p"):
        password=arg
    elif opt in ("-t"):
        target=arg
    elif opt in ("-r"):
        repSet=arg
    elif opt in ("-P"):
        port=arg
    elif opt in ("-d"):
        database_name=arg
    elif opt in ("-c"):
        collection_name=arg
    elif opt in ("-s"):
        sourceDir=arg
    elif opt in ("-f"):
        xmlSource=arg
    elif opt in ("--level"):
        print "Log Level set to : ", arg
        arg = arg.upper() 
        if not arg in ("DEBUG", "WARN", "INFO"):
            print "Invalid logging level specified"
            exit(2)
        else:
            logging.getLogger().setLevel(arg)
    elif opt in ("-h"):
        usage()
        exit()
    else:
        usage()
        exit(2)

jobs = []


logging.info("Initial Global Var information")
logging.info("Target: %s" % target)
logging.info("RepSet: %s" % repSet)
logging.info("Port: %d" % port)
logging.info("Username: %s" % username)
logging.info("Database: %s" % database_name)
logging.info("Collection: %s" % collection_name)
logging.info("SourceDir: %s" % sourceDir)
logging.info("SourceFile: %s" % xmlSource)


#Full path to the uncompressed XML source file from enron edrm v2 dataset
#Open it as a file object and then generate a dict from xml using xmltodict
#Finally grab a handle to the document objects for easier referencing
#xmlSource = sys.argv[1]
xmlFile = open(xmlSource)
xmldict = xmltodict.parse(xmlFile)
xmldict_root = xmldict["Root"]["Batch"]["Documents"]["Document"]
dictLength = len(xmldict_root)
logging.info("Doc Count: %d" % dictLength)

#Maintain the source dir for use with file objects - This needs to become a parameter with getops
sourceDir = "/enron/edrm-enron-v2"

def mongoConnector():
    connection = MongoClient(target,port,replicaSet=repSet,serverSelectionTimeoutMS=5000,connectTimeoutMS=5000)
    connection.admin.authenticate(username,password)
    return connection

def pretty(messyDict):
    return json.dumps(messyDict,sort_keys=True,indent=4, separators=(',', ': '))

def generateMongoDoc(increment):
    mongoDoc = OrderedDict()
    mongoDoc["_id"] = xmldict_root[increment]["@DocID"]
    mongoDoc["From"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#From")
    mongoDoc["To"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#To")
    mongoDoc["Date"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#DateSent")
    mongoDoc["Attachment"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#HasAttachments")
    mongoDoc["AttachmentName"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#AttachmentNames")
    mongoDoc["AttachmentCount"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#AttachmentCount")
    mongoDoc["Subject"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#Subject")
    mongoDoc["CC"] = extractNamedTag(xmldict_root[increment]["Tags"]["Tag"],"#CC")
    mongoDoc["Body"] = extractEmailBody(xmldict_root[increment]["Files"]["File"])

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

def buildMongoGfDoc(position):
    mongoDoc = OrderedDict()
    mongoDoc["_id"] = xmldict_root[position]["@DocID"]
    mongoDoc["FileName"] = extractNamedTag(xmldict_root[position]["Tags"]["Tag"], "#FileName")
    mongoDoc["Size"] = extractNamedTag(xmldict_root[position]["Tags"]["Tag"], "#FileSize")
    mongoDoc["DateCreated"] = extractNamedTag(xmldict_root[position]["Tags"]["Tag"], "#DateCreated")
    mongoDoc["DateModified"] = extractNamedTag(xmldict_root[position]["Tags"]["Tag"], "#DateModified")
    for i in xmldict_root[position]["Files"]["File"]:
        if i["@FileType"] == "Native":
            mongoDoc["FilePath"] = sourceDir + "/" + i["ExternalFile"]["@FilePath"] + "/" + i["ExternalFile"]["@FileName"]
            return mongoDoc

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
            requestList.append(InsertOne(generateMongoDoc(position))) 
            position = position + 1
            #print pretty(xmldict_root[position])
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
            requestList.append(InsertOne(generateMongoDoc(position)))
            position = position + 1
        if len(requestList) > 0 :
            results = batchSaveToDB(requestList)
            #print results
            #print requestList
            print results.bulk_api_result
            print results.acknowledged
        requestList = []
print pretty(xmldict_root[0])
position = 0

#Iterate the dictionary one more time to pull out all the attachments
while position != dictLength:
    if xmldict_root[position]["@DocType"] == "File"  and  xmldict_root[position]["@MimeType"] != 'application/octet-stream':
        mongoDoc = buildMongoGfDoc(position)
        print pretty(mongoDoc)
        mongoFile = open(mongoDoc["FilePath"])
        gresults = grid.put(mongoFile, _id=mongoDoc["_id"], source=mongoDoc)
    position = position + 1 
    

