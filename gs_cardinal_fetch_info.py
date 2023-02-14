#!/usr/bin/python3

############################################################
#                                                          #
#  This script is to fetch meta info from genestack study  #
#                                                          #
############################################################

################
#### ReadMe ####
################

'''
USAGE:\tgs_excel.py -i <sample id> 

Arguments:
\t-i, --id              sample source id

Flags:
\t-h, --help            help information
\t-v, --version         version information

Note:
\t1. sample tsv file should be in S3 bucket

Developer:
\tFei Sang
\tHuman Genetics Informatics
\tWellcome Genome Campus
\tHinxton, Cambridge, CB10 1DR
\tUnited Kingdom
''' 

##########################
#### Global Variables ####
##########################

TOKEN = "62cd64ed36cc500cde79c7690b9f74c6fd3e8a84"
GS = "genestack.sanger.ac.uk"
GSSTUDY = "GSF2353053"
SAMPLEGROUP = "GSF2353054"

#####################
#### subfunction ####
#####################

def versions():
    verStr = "Program:\tgs_cardinal_fetch_info.py\nVersion:\t1.0"
    print(verStr)

def usageInfo():
    versions()
    print(__doc__)

def welcomeWords():
    welWords = "Welcome to use tgs_cardinal_fetch_info.py"
    print("*"*(len(welWords)+20))
    print("*"," "*(7),welWords," "*(7),"*")
    print("*"*(len(welWords)+20))

def runTime(strTmp):
    runningTime = str(datetime.now())
    runningTime = runningTime.split(".")[0]
    print(runningTime,">>>>",strTmp)


#######################
#### main function ####
#######################

# system module
import sys, getopt
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# process module
import os
import time
import sample_user
from sample_user.rest import ApiException
from pprint import pprint

def main(argvs):
    #------------#
    # parameters #
    #------------#
    sampleID = ""

    try:
        opts, args = getopt.getopt(argvs,"vhi:",["version","help","id="])
        if len(opts) == 0:
            print("Please use the correct arguments, for usage type -h")
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
        elif opt in ("-i", "--id"):
            sampleID = arg
        else:
            assert False, "unhandled option"

    if sampleID == "": 
        sys.exit("Please use the correct arguments, option -i is missing")

    #------------#
    # processing #
    #------------#
    welcomeWords()
    runTime("1. Connect to GS, please waiting ...")

    os.environ["PRED_SPOT_HOST"] = GS
    os.environ["PRED_SPOT_TOKEN"] = TOKEN
    os.environ["PRED_SPOT_SCHEME"] = "https"
    os.environ["PRED_SPOT_VERSION"] = "default-released"

    api_instance = sample_user.SampleSPoTApi()
    filterstr = '"Sample Source ID"=' + '"' + sampleID + '"'

    # 0030007490009

    try:
        api_response = api_instance.search_samples(filter=filterstr)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SampleSPoTApi->get_sample: %s\n" % e)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])