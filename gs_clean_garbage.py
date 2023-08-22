#!/usr/bin/python3

#############################################################
#                                                           #
#  This script is to import the donor meta data and/or      #
#  the library meta data to the existing study on GeneStack #
#                                                           #
#############################################################

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
USAGE:\t{script_name} -r <the output report>

Arguments:
\t-r, --report          the report file path of all the studies in genestack

Optional:

Flags:
\t-h, --help            help information
\t-v, --version         version information

Note:
\t1. Fetch all the meta data of all the studies

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
    verStr = f"Program:\t{script_name}\nVersion:\t1.0"
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
import integration_curator

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
    reportOut = ""

    try:
        opts, args = getopt.getopt(argvs,"vhr:",["version","help","report="])
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
        elif opt in ("-r", "--report"):
            reportOut = arg
        else:
            assert False, "unhandled option"

    if reportOut == "": sys.exit("Error! Please use the correct arguments, option -r is missing")

    #------------#
    # processing #
    #------------#
    welcomeWords()

    os.environ["PRED_SPOT_HOST"] = GS
    os.environ["PRED_SPOT_TOKEN"] = TOKEN
    os.environ["PRED_SPOT_SCHEME"] = "https"
    os.environ["PRED_SPOT_VERSION"] = "default-released"

    pd.options.display.max_colwidth = 100

    runTime("1. Fetching all the studies in the genestack, please wait ...")
    apiInstance = study_curator.StudySPoTApi()
    fieldStr = "original_data_included"

    try:
        apiResponse = apiInstance.search_studies(returned_metadata_fields = fieldStr)
        df_study = pd.json_normalize(apiResponse.data)
    except study_curator.rest.ApiException as e:
        print("Exception when calling StudySPoTApi->search_studies: %s\n" % e)

    runTime("2. Fetching the number of donors in each study, please wait ...")
    apiInstance = integration_curator.SampleIntegrationApi()
    fieldStr = "minimal_data"

    df_study = df_study[['genestack:accession', 'Study Title']]
    
    # test datasets
    #df_study = df_study.head(2)
    #df_study = df_study[df_study['genestack:accession'].isin(['GSF2353053', 'GSF2353296'])]
    
    for index, row in df_study.iterrows():
        try:
            apiResponse = apiInstance.get_samples_by_study(id=row['genestack:accession'], page_limit=1, returned_metadata_fields = fieldStr)
            df_study.at[index, 'Donor Number'] = apiResponse.meta.pagination.total
        except integration_curator.rest.ApiException as e:
            print("Exception when calling SampleIntegrationApi->get_samples_by_study: %s\n" % e)

    df_study['Donor Number'] = df_study['Donor Number'].astype(int)

    runTime("3. Fetching the number of libraries in each study, please wait ...")
    apiInstance = integration_curator.LibraryIntegrationApi()

    for index, row in df_study.iterrows():
        try:
            apiResponse = apiInstance.get_groups_by_study(id=row['genestack:accession'])
            libgroups = apiResponse
            if not libgroups:
                df_study.at[index, 'Library Number'] = 0
            else:
                libnum = 0
                for group in libgroups:
                    apiResponse = apiInstance.get_library_links_to_samples(id=group.item_id, page_limit=1)
                    libnum = libnum + apiResponse.meta.pagination.total
                df_study.at[index, 'Library Number'] = libnum
        except integration_curator.rest.ApiException as e:
            print("Exception when calling SampleIntegrationApi->get_samples_by_study: %s\n" % e)

    df_study['Library Number'] = df_study['Library Number'].astype(int)

    df_study.to_csv(reportOut, sep = "\t", index = False)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])