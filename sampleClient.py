__author__ = 'pli'
#!/usr/bin/python

import sys
import os
import glob
import requests
import time
from datetime import datetime
import json
import podiumApiUtils as utils

# Global Variables
PodiumApp = dict(
    hostname="<host>",
    port="8080",
    app="podium",
    appuser="<appuser>",
    apppasswd="<apppasswd>!",
    database="podium_md",
    dbuser="postgres",
    dbpasswd="pwd"
)

SourceId=2
externalEntityId=1
internalEntityId=2
dirToFDL = "/path/to/FDLTest.txt"
######################################
# Name: Main() Function
# Input: None
# Output: None
# Function: Main entry to the program
######################################
def main():
    # Login PodiumApp & start REST request session
    print "Starting REST session ..."
    s = utils.startRestSession(PodiumApp)

    # Pprocess start
    starttime = time.time()

    # Sample call#0: Create source/entity metadata using an FDL
    entityinfo = utils.createEntityMeta(PodiumApp, s, dirToFDL)
    print "-------------Succesfully created new entity-----------"
    print entityinfo

    # Sample call#1: Get entities for the provided source id
    entities = utils.getEntityObjectList(PodiumApp,s,SourceId)
    print "------------entities for source with id of %d---------" % SourceId
    print entities

    # Sample call#2: Get all external sources
    print "------------All external Sources---------"
    extSources = utils.getAllExternalSources(PodiumApp,s)
    print extSources

    # Sample call#3: Get all external entities
    print "------------All external entities---------"
    extEntities = utils.getAllExternalEntities(PodiumApp,s)
    print extEntities

    # Sample call#4: Load data for entity with a given id via an asynchornous call
    print "------------Kicking off load data for entity with id %d---------" %externalEntityId
    utils.loadEntity(PodiumApp, s, externalEntityId)

    # Sample call#5: Poll to check for load to finish
    while not utils.checkLoadFinished(utils.getLoadLogs(PodiumApp,s,externalEntityId)):
        time.sleep(10)

    # Sample call#6: Export data for provided entity id
    print "------------Exporting entity data---------"
    utils.exportEntityData(PodiumApp, s, internalEntityId)

    # Pprocess End
    endtime = time.time()

    # Calculate Total Runtime:
    print "\nTotal Pprocessing Time: %f seconds" % (endtime - starttime)

if __name__ == "__main__":
    main()