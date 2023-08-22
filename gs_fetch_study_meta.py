#!/usr/bin/python3

############################################################
#                                                          #
#  This script is to fetch study meta info from genestack  #
#                                                          #
############################################################

#######################
#### system module ####
#######################

import os
import sys, getopt
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

script_name = os.path.basename(__file__)

################
#### ReadMe ####
################

readME = f'''
USAGE:\t{script_name} -s <study accession or all>

Arguments:

Optional:
\t-s, --gsacc           study accession in genestack or (all) the studies

Flags:
\t-h, --help            help information
\t-v, --version         version information

Note:
\t1. Fetch all the existing studies, and extract accessions and titles 

Developer:
\tFei Sang
\tHuman Genetics Informatics
\tWellcome Genome Campus
\tHinxton, Cambridge, CB10 1DR
\tUnited Kingdom
''' 

#####################
#### subfunction ####
#####################

def versions():
    verStr = f"Program:\t{script_name}\nVersion:\t1.1"
    print(verStr)

def usageInfo():
    versions()
    print(readME)

def welcomeWords():
    welWords = f"Welcome to use {script_name}"
    print("*"*(len(welWords)+20))
    print("*"," "*(7),welWords," "*(7),"*")
    print("*"*(len(welWords)+20))

def runTime(strTmp):
    runningTime = str(datetime.now())
    runningTime = runningTime.split(".")[0]
    print(runningTime,">>>>",strTmp)

########################
#### process module ####
########################

import pandas as pd
import json
import study_curator

##########################
#### Global Variables ####
##########################

TOKEN = "62cd64ed36cc500cde79c7690b9f74c6fd3e8a84"
GS = "genestack.sanger.ac.uk"

#######################
#### main function ####
#######################

def main(argvs):
    #------------#
    # parameters #
    #------------#

    gsStudyAcc = ""

    try:
        opts, args = getopt.getopt(argvs,"vhs:",["version","help","gsacc="])
        if len(opts) == 0:
            usageInfo()
            sys.exit(2)
    except(getopt.GetoptError):
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-v", "--version"):
            verbose = True
            versions()
            sys.exit()
        elif opt in ("-h", "--help"):
            usageInfo()
            sys.exit()
        elif opt in ("-s", "--gsacc"):
            gsStudyAcc = arg
        else:
            assert False, "unhandled option"

    #------------#
    # processing #
    #------------#
    welcomeWords()

    os.environ["PRED_SPOT_HOST"] = GS
    os.environ["PRED_SPOT_TOKEN"] = TOKEN
    os.environ["PRED_SPOT_SCHEME"] = "https"
    os.environ["PRED_SPOT_VERSION"] = "default-released"

    pd.options.display.max_colwidth = 100

    runTime("1. Fetching all the studies, please wait ...")
    apiInstance = study_curator.StudySPoTApi()

    fieldStr = "original_data_included"

    # 0030007538312, 0030007548823
    try:
        apiResponse = apiInstance.search_studies(returned_metadata_fields = fieldStr)
        df_study = pd.json_normalize(apiResponse.data)
        
        if gsStudyAcc == "all":
            print(df_study[['genestack:accession','Study Title']])
        else:
            df_acc = df_study[df_study['genestack:accession']==gsStudyAcc]
            print(df_acc[['genestack:accession','Study Title']])

    except study_curator.rest.ApiException as e:
        print("Exception when calling StudySPoTApi->search_studies: %s\n" % e)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])