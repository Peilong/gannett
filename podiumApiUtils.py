__author__ = 'pli'
#!/usr/bin/python

########################################################
# This Podium REST API library requires basic Podium App
# information. For example:
# PodiumApp = dict(
#     hostname="100.200.100.200",
#     port="8080",
#     app="podium",
#     appuser="root",
#     apppasswd="passwd",
#     database="podium_db",
#     dbuser="postgres",
#     dbpasswd="passwd"
# )
########################################################

# Import dependencies and libs
import sys
import requests
import time
import json
from datetime import datetime
from boto.s3.connection import S3Connection
import re


########################################################
# Name: getEntityInfo(PodiumApp, restSession, EntityId)
# Input: 1. Podium App Info: dict()
#        2. A REST session Object
#        3. A single Entity ID
# Output: A list of Entity Info including props
#         (JSON formatted)
# Function: get JSON formatted entity info from
#           a single entity ID
########################################################
def getEntityInfo(PodiumApp, restSession, EntityId):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/%d" % EntityId
    entityInfo = restSession.get(url + sourceType)
    if entityInfo == "":
        print "Get Entity Info Failed!"
        print "Preprocess is aborted ..."
        exit(1)
    else:
        return entityInfo.json()


############################################################
# Name: getEntityProps(entityInfo)
# Input: 1. A JSON formatted entity info
# Output: A list of Entity Properties: list(JSON1, JSON2 ..)
# Function: Get a list of entity props from
#           JSON formatted entity info
############################################################
def getEntityProps(entityInfo):
    entityProps = entityInfo["props"]
    if entityProps == "":
        print "Get Entity Properties Failed!"
        print "Preprocess is aborted ..."
        exit(1)
    else:
        return entityProps


##########################################################
# Name: changeEntityPropByName(Props, PropName, Value)
# Input: 1. A list of entity props: list(JSON1, JSON2 ..)
#        2. Propertiy Name that need to be updated
#        3. A new string value that replaces the old one
# Output: A list of NEW entity props: list(JSON1, JSON2..)
# Function: Get a new string of entity props from
#           a single entity ID
##########################################################
def changeEntityPropByName(Props, PropName, Value):
    for idx, prop in enumerate (Props):
        if prop['name'] == PropName:
            if prop['value'] == Value:
                print "No Entity Prop Has Changed!"
                print "Preprocess is aborted ..."
                exit(1)
            else:
                prop['value'] = Value
    return json.dumps(Props)


###################################################################
# Name: getAllExternalEntities()
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
# Output: List of external entities
# Function: returns a list of all entities of type 'external'.
###################################################################
def getAllExternalEntities(PodiumApp, restSession):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/external"
    entityObjectList = restSession.get(url + sourceType)
    if entityObjectList == "":
        print "EnityObjectList is empty!"
        print "Process is aborted ..."
        exit(1)
    elif "Error" in entityObjectList.text:
        print "Get Entity Object List Error"
        exit(1)
    else:
        return  entityObjectList.json()


###################################################################
# Name: getAllInternalEntities()
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
# Output: List of external entities
# Function: returns a list of all entities of type 'external'.
###################################################################
def getAllInternalEntities(PodiumApp, restSession):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/internal"
    entityObjectList = restSession.get(url + sourceType)
    if entityObjectList == "":
        print "EnityObjectList is empty!"
        print "Process is aborted ..."
        exit(1)
    elif "Error" in entityObjectList.text:
        print "Get Entity Object List Error"
        exit(1)
    else:
        return  entityObjectList.json()


###################################################################
# Name: getAllExternalSources()
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
# Output: List of external sources
# Function: returns a list of all sources of type 'external'.
###################################################################
def getAllExternalSources(PodiumApp, restSession):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "source/external"
    sourceObjectList = restSession.get(url + sourceType)
    if sourceObjectList == "":
        print "sourceObjectList is empty!"
        print "Process is aborted ..."
        exit(1)
    elif "Error" in sourceObjectList.text:
        print "Get External Source Object List Error"
        exit(1)
    else:
        return  sourceObjectList.json()


###################################################################
# Name: updateEntityProp(PodiumApp, restSession, entityId, newProp)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. A single Entity ID: int
#        4. The new property: str
# Output: None
# Function: Update the old entity property with the NEW property.
###################################################################
def updateEntityProp(PodiumApp, restSession, entityId, newProp):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/updProps/%d" % entityId
    headers = {'content-type': 'application/json'}
    ret = restSession.put(url + sourceType,
                          data=newProp, headers=headers)
    if "Error" in (ret.text):
        print "Update Entity Property Failed!..."
        print "Preprocess is aborted..."
        exit(1)
    else:
        print "Update Entity Property Success!!"


###################################################################
# Name: getEntityObjectList(PodiumApp, restSession, EntitySourceId)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. A single Entity Source ID: int
# Output: A list of Entity Objects: dict() (JSON formatted)
# Function: get a list of entity objects from
#           a single entity source ID
###################################################################
def getEntityObjectList(PodiumApp, restSession, EntitySourceId):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/external/entitiesBySrc/%d" % EntitySourceId
    entityObjectList = restSession.get(url + sourceType)
    if entityObjectList == "":
        print "EnityObjectList is empty!"
        print "Preprocess is aborted ..."
        exit(1)
    elif "Error" in entityObjectList.text:
        print "Get Entity Object List Error"
        exit(1)
    else:
        return  entityObjectList.json()


###########################################################
# Name: getEntityId(entityObject)
# Input: A JSON-formatted (list of) entity objects: dict()
# Output: A single entity ID: int
# Function: get a single entity ID from
#           a JSON formatted entity object
###########################################################
def getEntityId(entityObject):
    entityObjectSubList = entityObject.get("subList")
    if not len(entityObjectSubList) == 1:
        print "It's not a single entity. Please try getEntityIdList()"
        print "Preprocess is aborted ..."
        exit(1)
    entityId = entityObjectSubList[0].get("id")
    if not entityId:
        print "Entity ID does not exist"
        print "Preprocess is aborted ..."
        exit(1)
    else:
        return entityId


########################################################
# Name: getEntityIdList(entityObjectList)
# Input: A JSON-formatted list of entity objects: dict()
# Output: A list of Entity IDs: list[int, int, ..]
# Function: get a list of entity IDs from
#           a list of entity objects.
########################################################
def getEntityIdList(entityObjectList):
    entityIdList = []
    entityObjectSubList = entityObjectList.get("subList")
    for idx, item in enumerate(entityObjectSubList):
        entityIdList.append(item.get("id"))
    if not entityIdList:
        print "Entity ID list is empty!"
        print "Preprocess is aborted ..."
        exit(1)
    else:
        return entityIdList


#######################################################
# Name: updateEntity(PodiumApp, restSession, EntityId)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. A single Entity ID: int
# Output: None
# Function: Update the Podium Postgres
#           DB with a single EntityId
#######################################################
def updateEntity(PodiumApp, restSession, EntityId):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    headers = {'content-type': 'application/json'}
    sourceType = "entity/loadDataForEntity/true"
    # Generate the payload
    payload = dict(
        loadTime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        entityId=str(EntityId))
    # Send PUT request
    ret = restSession.put(url + sourceType,
                          data=json.dumps(payload), headers=headers)
    if "Error" in (ret.text):
        print "Update Entities Failed!..."
        print "Preprocess is aborted ..."
        exit(1)
    else:
        print "Update single entity: job submitted Successfully."
        return


###########################################################
# Name: updateEntities(PodiumApp, restSession, EntityList)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. A list of Entity IDs: list[ID1, ID2, ..]
# Output: None
# Function: Update the Podium Postgres
#           DB with a list of EntityIds
###########################################################
def updateEntities(PodiumApp, restSession, EntityList):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    headers = {'content-type': 'application/json'}
    sourceType = "entity/loadDataForEntities/true"
    # Generate the payloadList
    payloadList = []
    for idx, entityId in enumerate(EntityList):
        payloadList.append( dict(
            loadTime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            entityId=str(entityId)) )
    # Send PUT request
    ret = restSession.put(url + sourceType,
                          data=json.dumps(payloadList), headers=headers)
    if "Error" in (ret.text):
        print "Update Entities Failed!..."
        exit(1)
    else:
        print "Update entities: job submitted successfully."
        return



########################################################
# Name: getExternalntityInfo(PodiumApp, restSession, EntityId)
# Input: 1. Podium App Info: dict()
#        2. A REST session Object
#        3. A single Entity ID
# Output: A list of Entity Info including props
#         (JSON formatted)
# Function: get JSON formatted entity info from
#           a single entity ID
########################################################
def getInternalEntityInfo(PodiumApp, restSession, EntityId):
    externalEntities = getAllInternalEntities(PodiumApp, restSession)
    entityObjectSubList = externalEntities.get("subList")
    for idx, item in enumerate(entityObjectSubList):
        if item.get("id") == EntityId:
            entityObject = item
    if not entityObject:
        print "Entity with provided id not found!"
        exit(1)
    else:
        return json.dumps(entityObject)

###########################################################
# Name: exportEntityData(PodiumApp, restSession, EntityId)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. Entity ID
# Output: None
# Function: exports data for provided entity
###########################################################
def exportEntityData(PodiumApp, restSession, EntityId):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    headers = {'content-type': 'application/json'}
    sourceType = 'cart/exportData'
    #get entity info
    entityInfo = getInternalEntityInfo(PodiumApp, restSession, EntityId)
    # Generate the export info object
    exportInfo = '{\"entity\":' + str(entityInfo)
    exportInfo = exportInfo +\
                 ',\"fieldObfuscations\":[],'+\
                 '\"exportLocation\":\"/tmp/podium/export\",'+\
                 '\"exportFolder\":\"podium_core_pd_user_<ExportTime>\",'+\
                 '\"loadStamps\":[1440607774071],'+\
                 '\"dataSelection\":\"LATEST\",'+\
                 '\"occurence\":\"ONE_TIME_IMMEDIATE\"}'
    # Send PUT request
    print exportInfo
    ret = restSession.put(url + sourceType,
                          data=exportInfo, headers=headers)
    if "Error" in (ret.text):
        print "export entity call Failed!..." + ret.text
        exit(1)
    else:
        print "Entity data exported successfully."
        return

###########################################################
# Name: addEntityToCart(PodiumApp, restSession, EntityId)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. Entity ID
# Output: None
# Function: adds the given entity to cart
###########################################################
def addEntityToCart(PodiumApp, restSession, EntityId):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    headers = {'content-type': 'application/json'}
    sourceType = "cart/addEntitiesFieldsToCart"
    # Generate the payloadList
    payloadList = []
    payloadList.append(str(EntityId))
    # Send PUT request
    ret = restSession.put(url + sourceType,
                          data=json.dumps(payloadList), headers=headers)
    if "Error" in (ret.text):
        print "Add Entity to Cart Failed!..."
        exit(1)
    else:
        print "Entity successfully added to cart."
        return


################################################################
# Name: getLoadLogsForEntity(PodiumApp, restSession, EntityList)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. Entity ID: int
# Output: The latest JSON formatted data load logs
# Function: Get the latest load entity log
################################################################
def getLoadLogsForEntity(PodiumApp, restSession, EntityId):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/loadLogs/%d" % EntityId
    # Call Podium API to get load logs
    logs = restSession.get(url + sourceType)
    # Get the subList from logs
    logsSubList = logs.json()['subList']
    # Sort the subList in ascending order, make sure the latest
    # log is on the bottom of the list
    sortedList = sorted(logsSubList, key=lambda k:k['id'])
    # Get the lastest log
    latestLog = sortedList[len(sortedList)-1]
    if "Error" in json.dumps(latestLog):
        print "Update Entities Failed!..."
        exit(1)
    elif json.dumps(latestLog) == '':
        print "No data load logs!!"
        exit(1)
    else:
        return latestLog


##################################################################
# Name: getLoadLogsForEntities(PodiumApp, restSession, EntityList)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. A list of Entity IDs: list(int, int, ..)
# Output: A list of the latest JSON formatted data load logs
#         list(JSON1, JSON2, ...)
# Function: Get the latest load entity log for each entity in the
#           entity list
##################################################################
def getLoadLogsForEntities(PodiumApp, restSession, EntityList):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    loadLogList = []
    for idx, entity in enumerate (EntityList):
        sourceType = "entity/loadLogs/%d" % entity
        # Call Podium API to get load logs
        logs = restSession.get(url + sourceType)
        # Get the subList from logs
        logsSubList = logs.json()['subList']
        # Sort the subList in ascending order, make sure the latest
        # log is on the bottom of the list
        sortedList = sorted(logsSubList, key=lambda k:k['id'])
        # Get the lastest log
        latestLog = sortedList[len(sortedList)-1]
        if "Error" in json.dumps(latestLog):
            print "Update Entities Failed!..."
            exit(1)
        elif json.dumps(latestLog) == '':
            print "No data load logs!!"
            exit(1)
        else:
            loadLogList.append(latestLog)
    return loadLogList


#############################################################
# Name: checkLoadFinishedForEntity(LoadLog)
# Input: 1. A JSON formatted data load logs (use getLoadLogs)
# Output: True - data load finished
#         False - data load not finished yet
# Function: Check if data load has finished
#############################################################
def checkLoadFinishedForEntity(LoadLog):
    if not LoadLog['status'] == 'FINISHED':
        if LoadLog['status'] == 'FAILED':
            print 'Load failed ...'
            exit(1)
        print "Still loading..."
        return False
    return True


#############################################################
# Name: checkLoadFinishedForEntities(LoadLogList)
# Input: 1. A JSON formatted data load logs (use getLoadLogs)
# Output: True - data load finished
#         False - data load not finished yet
# Function: Check if data load has finished
#############################################################
def checkLoadFinishedForEntities(LoadLogList):
    for idx, loadLog in enumerate(LoadLogList):
        if not loadLog['status'] == 'FINISHED':
            if loadLog['status'] == 'FAILED':
                print 'Load failed ...'
                exit(1)
            print "Still loading..."
            return False
    return True


#############################################################
# Name: createEntityMeta(PodiumApp, restSession, dirToMetaFile)
# Input: 1. Podium App Info: dict()
#        2. A REST session object: object
#        3. Directory to your FDL file: str
# Output: A dict() formatted entity information
#         dict(name = 'newentity'
#              id = '1234'
#         )
# Function: create entity by using FDL file and return the
#           new entity ID and name
#############################################################
def createEntityMeta(PodiumApp, restSession, dirToMetaFile):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    sourceType = "entity/ldMeta"
    headers = {'context-type': 'multipart/form-data'}
    files = {'file': ('file', open(dirToMetaFile, 'rb'))}
    ret = restSession.post(url + sourceType, files=files, headers=headers)
    if "Error" in (ret.text):
        print ret.text
        print "Create entity metadata failed!..."
        exit(1)
    elif "exists" in ret.text:
        match = re.search(r'"(.*)"',
                          ret.text)
        print match.group(1)
    else:
        match = re.search(r'Metadata has been succesfully '+
                          'loaded from given payload,'+
                          ' to create data source with name: '+
                          '(\S+) and id:(\S+)"',
                          ret.text)
        entity = dict(name=match.group(1),
                      id=match.group(2))
        print "Create entity metadata successfully"
        return entity


################################################
# Name: startRestSession(PodiumApp)
# Input: Podium App Info: dict()
# Output: A REST request session object
# Function: Start a REST session and login
################################################
def startRestSession(PodiumApp):
    url = "http://" + PodiumApp.get("hostname") + ":" + \
          PodiumApp.get("port") + "/" + PodiumApp.get("app") + "/"
    securityUrl = url + "j_spring_security_check"
    # Start a request session
    s = requests.session()
    login_data = dict(
        j_username=PodiumApp.get("appuser"),
        j_password=PodiumApp.get("apppasswd")
    )
    # Post a login request to Podium App
    loginResponse = s.post(securityUrl, data=login_data)
    loginResponseText = loginResponse.text
    # If login success, then return the REST request session object
    if not "login" in loginResponseText:
        print "Start REST Request Session Successfully!"
        return s
    # If not login, then report error and exit
    else:
        print "Invalid username or password....!!"
        exit(1)


#########################################
# Name: getS3Bucket()
# Input: None, use default env var
#        should export ACCESS id and key
# Output: The bucket handler
# Function: Start a S3 connection session
#           and return the bucket handler
#########################################
def getS3Bucket():
    conn = S3Connection()
    bucket = conn.get_bucket('gci-data-lake')
    return bucket


###############################################
# Name: getS3DataList(Bucket)
# Input: S3 bucket handler: object
# Output: A list of data files: list[f1, f2 ..]
# Function: Get the S3 data list
###############################################
def getS3DataList(Bucket):
    filelist = []
    for key in Bucket.list():
        filelist.append(key.name.encode('utf-8'))
    datafilelist = []
    for idx, file in enumerate(filelist):
        match = re.search(r'omniture/gannett/day.*0.gz', file)
        if match:
            datafilelist.append(file)
    return datafilelist


##################################################
# Name: getGannettDataInfo(DataFileDir)
# Input: A string of data directory name: str
# Output: A dict of data info
#         dict(
#             day='day',
#             hour='hour'
#             file='file'
#         )
# Function: Get the regex matched file info
##################################################
def getGannettDataInfo(DataFileDir):
    match = re.search(r'omniture/gannett/day=(\S+)/hour=(\S+)/(.*)',
                      DataFileDir)
    return dict(day=match.group(1),
                hour=match.group(2),
                file=match.group(3))


######################################################
# Name: testIfNeedLoad(LastButOneRecord, LastRecord)
# Input: 1. A dict of data info of last record
#        2. A dict of data info of new record
#         dict(
#             day='day',
#             hour='hour'
#             file='file'
#         )
# Output: True - we can load data for the new hour
#         False - we cannot load data because we are
#                 still in the same hour slot.
# Function: Get a binary signal showing if load data
######################################################
def testIfNeedLoad(LastRecord, NewRecord):
    if not LastRecord.get('hour')==NewRecord.get('hour'):
        return True
    else:
        return False


######################################################
# Name: testIfNeedLoad(LastButOneRecord, LastRecord)
# Input: 1. A dict of data info of last but one record
#         dict(
#             day='day',
#             hour='hour'
#             file='file'
#         )
# Output: True - we can load data for the new hour
#         False - we cannot load data because we are
#                 still in the same hour slot.
# Function: Get a binary signal showing if load data
######################################################
def genSrcFileGlob(DataInfo):
    return "/omniture/gannett/day="+\
           DataInfo.get('day')+\
           "/hour="+DataInfo.get('hour')+\
           "/*_0.gz"


#######################################
# Name: printStepHeader(number, title)
# Input: 1. The step number integer
#        2. The step title String
# Output: The string of step header
# Function: Print the step header info
#           to console
#######################################
def printStepHeader(number, title):
    titlecat = "* Step "+ \
               str(number)+ \
               ": "+ \
               title+ \
               " *"
    print "\n" + \
          len(titlecat)*'*'+ \
          "\n"+ \
          titlecat+ \
          "\n"+ \
          len(titlecat)*'*'
