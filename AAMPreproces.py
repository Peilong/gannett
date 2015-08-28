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
# Should not be changed for production
PodiumApp = dict(
    hostname="172.21.6.66",
    port="4675",
    app="podium",
    appuser="podium",
    apppasswd="nvs2014!",
    database="podium_md_mapr_prod",
    dbuser="postgres",
    dbpasswd="password"
)

AAMSourceId=129
AAMPrepSourceId=143

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

    # Get S3 bucket handler
    print "Getting S3 bucket handler ..."
    bucket = utils.getS3Bucket()

    # Get initial S3 data list under /omniture/gannett
    currentDataList = utils.getS3DataList(bucket)

    # Get current data list state
    currentState = utils.getGannettDataInfo(currentDataList[len(currentDataList)-1])

    #lastState = dict(day='2015-08-25', hour='19', file='AAM_CDF_1244_000000_0.gz')

    # Get entity ID for AAMSourceId
    entityId = utils.getEntityId(utils.getEntityObjectList(PodiumApp,s,AAMSourceId))

    # Get entity IDs for AAMPrepSourceId
    entityList  = utils.getEntityIdList(utils.getEntityObjectList(PodiumApp,s,AAMPrepSourceId))

    while True:
        # Assign last state with current state every loop
        lastState = currentState

        # Update current S3 data list under /omniture/gannett
        utils.printStepHeader(1, "Update S3 data file list")
        currentDataList = utils.getS3DataList(bucket)

        # Update current data list state
        currentState = utils.getGannettDataInfo(currentDataList[len(currentDataList)-1])

        # Test if current state still equals to last state
        if utils.testIfNeedLoad(lastState, currentState):
            # Preprocess start
            starttime = time.time()

            # Get entity's old properties
            utils.printStepHeader(2, "Retrieve old props")
            oldProp = utils.getEntityProps(utils.getEntityInfo(PodiumApp,s,entityId))

            # Generate NEW properties with last state
            utils.printStepHeader(3, "Generate new props")
            newProp = utils.changeEntityPropByName(
                oldProp,
                "src.file.glob",
                utils.genSrcFileGlob(lastState) )

            # Update entity properties
            utils.printStepHeader(4, "Update entity props")
            utils.updateEntityProp(PodiumApp, s, entityId, newProp)

            # Load data for entity Adobe_Audience_Manager
            utils.printStepHeader(5, "Load data for entity")
            utils.updateEntity(PodiumApp, s, entityId)

            # Check if load finished
            utils.printStepHeader(6, "Wait for entity loading finished")
            while not \
                utils.checkLoadFinishedForEntity(utils.getLoadLogsForEntity(PodiumApp,s,entityId)):
                time.sleep(60)
            print "Loading data for entity finished"

            # Execute Hive Script
            utils.printStepHeader(7, "Execute Hive Scripts")
            print "Hive step 1 ..."
            os.system("hive -f step1.sql")

            print "Hive step 2 ..."
            os.system("hive -f step2.sql")

            print "Hive step 3 ..."
            os.system("hive -f step3.sql")

            print "Hive step 4 ..."
            os.system("hive -f step4.sql")

            print "Hive step 5 ..."
            os.system("hive -f step5.sql")

            # Load data for entities
            utils.printStepHeader(8, "Load data for entities")
            utils.updateEntities(PodiumApp, s, entityList)

            # Check if load finished
            utils.printStepHeader(9, "Wait for entities loading finished")
            while not \
                utils.checkLoadFinishedForEntities(
                        utils.getLoadLogsForEntities(PodiumApp,s,entityList)):
                time.sleep(60)
            print "Loading data for entities finished"

            # Preprocess End
            endtime = time.time()

            # Calculate Total Runtime:
            print "\nTotal Preprocessing Time: %f seconds" % (endtime - starttime)

        else:
            print "%s: No new data added" % datetime.now()
            print "Waiting for next checking cycle ..."
            time.sleep(300)

if __name__ == "__main__":
    main()