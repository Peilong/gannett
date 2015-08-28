__author__ = 'pli'

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
    hostname="tinkerbell.podiumdata.com",
    port="8080",
    app="podium",
    appuser="podium",
    apppasswd="nvs2014!",
    database="podium_md_mapr_prod",
    dbuser="postgres",
    dbpasswd="password"
)

######################################
# Name: Main() Function
# Input: None
# Output: None
# Function: Main entry to the program
######################################
def main():
    s = utils.startRestSession(PodiumApp)
    metadatafile = '/Users/pli/Desktop/file.txt'
    utils.createEntityMeta(PodiumApp,s,metadatafile)

if __name__ == "__main__":
    main()