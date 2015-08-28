# How To Use AAM Automation Tools

The AAM automation uses python scripts and requires multiple external python libraries to do the magic. These libraries are: requests, boto, and json. If these libraries are not default in your system, please install them before using. To install these libraries, simply type in “pip install boto” in your terminal console. If you do not have pip installed, go ahead install it.

## Section 1: The Podium API Utilities Library

“podiumApiUtils.py” is a wrapper library that provides better Podium APIs usage experience. The library currently provides quite a few functions that manipulate Podium entity APIs. This library is also a good place for future Podium wrappers. 

Detailed usage information of each function is well commented in lines, which includes the inputs parameters, output format and the functionalities. 

Here is a sample code that uses the wrapper library for data loading and etc. 

<pre><code>
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
    apppasswd="<apppasswd>",
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
    
</code></pre>


## Section 2: AAM Automation Logic

The logic diagram of AAM automation is attached. 

The basic steps are described as following:
1. Define the PodiumApp parameters. Basically, you need to put in the host name and port where podium is installed. Change the app user name, password and etc. 
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

2. Start REST Session and login to Podium App by using 'startRestSession(PodiumApp)'

3. Get S3 bucket handler by using ‘getS3Bucket'

4. Get the single entity ID for Adobe Audience Manager by using 'getEntityId'

5. Get the multiple entity ID list for Adobe Audience Prep by using 'getEntityList'
 
6. Since S3 does not provide an ‘event’ mechanism, we need to polling the list of S3 bucket to see if there is new data with the latest hour time stamp comes in. This is a continuous polling mechanism and the frequency of the polling delay should be set. 

7. If new data comes in, then we should update the src.file.glob property for the entity and load the new data. Use ‘updateEntityProp’ to update the property and use ‘updateEntity’ to load new data in the entity.

8. Since data loading to entity takes a couple of minutes depending on the data size, we need to wait the data loading finished then proceed to the next step. Use ‘checkLoadFinishedForEntity’ to check the loading progress.

9. Run the HIVE scripts within python context by using os.system(‘hive -f hiveScriptName.sql')

10. Load data for Adobe Audience Prep by using ‘updateEntityList’ since Adobe Audience Prep has a list of entities.

11. Again, wait the data loading until it’s finished. 

12. This will conclude a whole cycle of AAM data preprocessing. Wait for the polling delay and enter the next checking cycle. 

