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
USAGE:\tgs_cardinal_fetch_info.py -i <sample id> -l <library id> -s <sample group id> -b <library group id>

Arguments:
\t-i, --id              sample source id
\t-l, --lib             library id

Optional:
\t-s, --samplegroup     sample group id
\t-b, --librarygroup    library group id

Flags:
\t-h, --help            help information
\t-v, --version         version information

Note:
\t1. Sample ID must be Sample Source ID, eg: 0030007490009, S2-999-90271
\t2. Library ID must be LCA Connect PCRXP, eg: DN952438D:B1, SQPP-12707-S:D1

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
    welWords = "Welcome to use gs_cardinal_fetch_info.py"
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
import os
import sys, getopt
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# process module
import sample_user

# library_user search function isn't working, error message said "Metadata ID must be specified"
import library_curator
import pandas as pd

def main(argvs):
    #------------#
    # parameters #
    #------------#
    sampleID = ""
    libraryID = ""
    sampleGroupID = ""
    libraryGroupID = ""

    try:
        opts, args = getopt.getopt(argvs,"vhi:l:s:b:",["version","help","id=","lib=","samplegroup=","librarygroup="])
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
        elif opt in ("-i", "--id"):
            sampleID = arg
        elif opt in ("-l", "--lib"):
            libraryID = arg
        elif opt in ("-s", "--samplegroup"):
            sampleGroupID = arg
        elif opt in ("-b", "--librarygroup"):
            libraryGroupID = arg
        else:
            assert False, "unhandled option"

    if sampleID == "": sys.exit("Error! Please use the correct arguments, option -i is missing")
    if libraryID == "": sys.exit("Error! Please use the correct arguments, option -l is missing")

    #------------#
    # processing #
    #------------#
    welcomeWords()

    os.environ["PRED_SPOT_HOST"] = GS
    os.environ["PRED_SPOT_TOKEN"] = TOKEN
    os.environ["PRED_SPOT_SCHEME"] = "https"
    os.environ["PRED_SPOT_VERSION"] = "default-released"

    runTime("1. Fetching sample meta by sample id, please waiting ...")
    api_instance = sample_user.SampleSPoTApi()
    filterstr = '"Sample Source ID"=' + '"' + sampleID + '"'
    fieldstr = "original_data_included"

    # 0030007490009, 0030007538312
    try:
        api_response = api_instance.search_samples(filter=filterstr, returned_metadata_fields=fieldstr)
        if api_response.meta.pagination.count == 0:
            printstr = "    |----> Error! " + sampleID + " is not found in genestack database!"
            runTime(printstr)
            printstr = "    |----> Running Stop!"
            runTime(printstr)
            sys.exit()
        else:
            df_api_sample = pd.DataFrame(api_response.data)
            df_api_sample.dropna(axis = 1, how = 'all', inplace = True)
            df_api_sample_filter = df_api_sample[df_api_sample['groupId'].notna()].reset_index()
            if len(df_api_sample_filter) == 1:
                df_api_sample_filter = df_api_sample_filter[['Sample Source ID', 'State', 'Viability', 'groupId']]
                printstr = "    |----> " + sampleID + " fetched"
                runTime(printstr)
            else:
                if sampleGroupID == "":
                    printstr = "    |----> Error! " + sampleID + " is found in multiple groups in genestack database!"
                    runTime(printstr)
                    for index, row in df_api_sample_filter.iterrows():
                        printstr = "        |----> Group Found: " + row['groupId']
                        runTime(printstr)
                    printstr = "    |----> Running Stop! You can try to privode sample group id by -s."
                    runTime(printstr)
                    sys.exit()
                else:
                    df_api_sample_filter = df_api_sample_filter[['Sample Source ID', 'State', 'Viability', 'groupId']]
                    df_api_sample_filter = df_api_sample_filter[df_api_sample_filter['groupId']==sampleGroupID]
                    printstr = "    |----> " + sampleID + " fetched"
                    runTime(printstr)
    except sample_user.rest.ApiException as e:
        print("Exception when calling SampleSPoTApi->search_samples: %s\n" % e)

    runTime("2. Fetching library meta by library id, please waiting ...")
    api_instance = library_curator.LibrarySPoTApi()
    filterstr = '"Library ID"=' + '"' + libraryID + '"'
    fieldstr = "original_data_included"

    # DN952438D:A1
    try:
        api_response = api_instance.search_libraries(filter=filterstr, returned_metadata_fields=fieldstr)
        if api_response.meta.pagination.count == 0:
            printstr = "    |----> Error! " + libraryID + " is not found in genestack database!"
            runTime(printstr)
            printstr = "    |----> Running Stop!"
            runTime(printstr)
            sys.exit()
        else:
            df_api_library = pd.DataFrame(api_response.data)
            df_api_library.dropna(axis = 1, how = 'all', inplace = True)
            df_api_library_filter = df_api_library[df_api_library['groupId'].notna()].reset_index()
            if len(df_api_library_filter) == 1:
                df_api_library_filter = df_api_library_filter[['Library ID', 'Sample Source ID', '24h', '48h', '72h', 'Frozen', 'None']]
                printstr = "    |----> " + libraryID + " fetched"
                runTime(printstr)
            else:
                if libraryGroupID == "":
                    printstr = "    |----> Error! " + libraryGroupID + " is found in multiple groups in genestack database!"
                    runTime(printstr)
                    for index, row in df_api_library_filter.iterrows():
                        printstr = "        |----> Group Found: " + row['groupId']
                        runTime(printstr)
                    printstr = "    |----> Running Stop! You can try to privode library group id by -b."
                    runTime(printstr)
                    sys.exit()
                else:
                    df_api_library_filter = df_api_library_filter[['Library ID', 'Sample Source ID', '24h', '48h', '72h', 'Frozen', 'None']]
                    df_api_library_filter = df_api_library_filter[df_api_library_filter['groupId']==libraryGroupID]
                    printstr = "    |----> " + libraryID + " fetched"
                    runTime(printstr)
    except library_curator.rest.ApiException as e:
        print("Exception when calling SampleSPoTApi->search_libraries: %s\n" % e)

    runTime("3. Combining results, please waiting ...")
    library_sampleIDs = df_api_library_filter['Sample Source ID'].values.item()
    if sampleID in library_sampleIDs:
        lib_index = library_sampleIDs.index(sampleID)
        library_24h = df_api_library_filter['24h'].values.item()
        library_48h = df_api_library_filter['48h'].values.item()
        library_72h = df_api_library_filter['72h'].values.item()
        library_frozen = df_api_library_filter['Frozen'].values.item()
        library_none = df_api_library_filter['None'].values.item()
        state_list = pd.DataFrame({'24h': library_24h,
                                   '48h': library_48h,
                                   '72h': library_72h,
                                   'Frozen': library_frozen,
                                   'None': library_none})
        sample_state = state_list.iloc[lib_index][state_list.iloc[lib_index] == True].index[0]

        # query dataframe to numpy array
        querystr = "`Sample Source ID`" + "==" + "'" + sampleID + "'"
        query_state = df_api_sample_filter.query(querystr)['State'][0]
        query_viability = df_api_sample_filter.query(querystr)['Viability'][0]
        if type(query_state) is list:
            sample_viability = query_viability[query_state.index(sample_state)]
        else:
            sample_viability = query_viability
        printstr = "    |----> " + sampleID + '  ' + libraryID + '  ' + sample_state + '  ' + sample_viability
        runTime(printstr)
    else:
        printstr = "    |----> Error! " + sampleID + " is not in " + libraryID + "!"
        runTime(printstr)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])