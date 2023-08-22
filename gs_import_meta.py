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
USAGE:\t{script_name} -s <study genestack accession> -d <donor meta tsv> -l <library meta tsv>

Arguments:
\t-s, --gsacc           study accession in genestack
\t-d, --donor           the path of donor meta tsv file

Optional:
\t-l, --library         the path of library meta tsv file

Flags:
\t-h, --help            help information
\t-v, --version         version information

Note:
\t1. the study has to be created first with the accession ID
\t2. the donor meta tsv file and the library meta tsv file should be in s3 bucket
\t3. make sure s3 bucket is private to genestack only
\t4. the library meta tsv file is linked to the donor meta tsv file via Sample Source ID

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

### stop here, cannot find job python api in genestack api


