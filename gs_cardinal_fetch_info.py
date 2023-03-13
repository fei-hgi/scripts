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
    verStr = "Program:\tgs_cardinal_fetch_info.py\nVersion:\t1.1"
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
import pandas as pd
import json
import integration_curator

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
    apiInstance = integration_curator.OmicsQueriesApi()

    studyFilterStr = '"genestack:accession"=' + '"' + GSSTUDY + '"'
    sampleFilterStr = '"Sample Source ID"=' + '"' + sampleID + '"'
    fieldStr = "original_data_included"

    # 0030007538312, 0030007548823
    try:
        apiResponse = apiInstance.search_samples(study_filter = studyFilterStr, sample_filter = sampleFilterStr, returned_metadata_fields = fieldStr)

        # study definitely is found, so log must report that study is found
        if len(apiResponse.log) == 1:
            printStr = "    |----> Error! " + sampleID + " is not found in genestack study (" + GSSTUDY + ")!"
            runTime(printStr)
            printStr = "    |----> Running Stop!"
            runTime(printStr)
            sys.exit()
        else:
            df_sample = pd.json_normalize(apiResponse.data)
            # rename column ids
            df_sample_colnames = list(df_sample.columns)
            for i in range(len(df_sample_colnames)):
                df_sample_colnames[i] = df_sample_colnames[i].split('.')[1]
            df_sample = df_sample.set_axis(df_sample_colnames, axis = 1)
            df_sample.dropna(axis = 1, how = 'all', inplace = True)
            df_sampleFilter = df_sample[df_sample['groupId'].notna()].reset_index()
            
            sampleNum = apiResponse.log[1].split(' ')[1]
            printStr = "    |----> Found " + sampleNum + " sample(s) in total"
            runTime(printStr)
            
            if len(df_sampleFilter) == 1:
                df_sampleFilter = df_sampleFilter[['Sample Source ID', 'State', 'Viability', 'groupId']]
                printStr = "    |----> " + sampleID + " is fetched in the group " + df_sampleFilter.at[0, 'groupId']
                runTime(printStr)
            else:
                if sampleGroupID == "":
                    printStr = "    |----> Error! " + sampleID + " is found in multiple groups in the cardinal study!"
                    runTime(printStr)
                    for index, row in df_sampleFilter.iterrows():
                        printStr = "        |----> Group Found: " + row['groupId']
                        runTime(printStr)
                    printStr = "    |----> Running Stop! You can try to privode sample group id by -s"
                    runTime(printStr)
                    sys.exit()
                else:
                    df_sampleFilter = df_sampleFilter[['Sample Source ID', 'State', 'Viability', 'groupId']]
                    df_sampleFilter = df_sampleFilter[df_sampleFilter['groupId']==sampleGroupID]
                    printStr = "    |----> " + sampleID + " is fetched in the group " + sampleGroupID
                    runTime(printStr)
    except integration_curator.rest.ApiException as e:
        print("Exception when calling OmicsQueriesApi->search_samples: %s\n" % e)

    runTime("2. Fetching library meta by library id, please waiting ...")
    apiInstance = integration_curator.LibraryIntegrationApi()
    
    # fetch all the library groups in the cardinal project GSF2353053
    try:
        apiResponse = apiInstance.get_groups_by_study(id = GSSTUDY)
        libraryGroups = []
        for apires in apiResponse:
            libraryGroups.append(apires.item_id)
    except integration_curator.rest.ApiException as e:
        print("Exception when calling LibraryIntegrationApi->get_groups_by_study: %s\n" % e)

    sampleFilterStr = '"Sample Source ID"=' + '"' + sampleID + '"'
    fieldStr = "original_data_included"

    # SQPP-136-S:B1, SQPP-12781-F:C1
    try:
        apiResponse = apiInstance.get_libraries_by_samples(filter = sampleFilterStr, returned_metadata_fields = fieldStr)

        if apiResponse.meta.pagination.count == 0:
            printStr = "    |----> Error! " + sampleID + " is not found in any study library!"
            runTime(printStr)
            printStr = "    |----> Running Stop!"
            runTime(printStr)
            sys.exit()
        else:
            df_library = pd.DataFrame(apiResponse.data)
            df_library.dropna(axis = 1, how = 'all', inplace = True)
            df_libraryFilter = df_library[df_library['groupId'].notna()].reset_index()
            if len(df_libraryFilter) == 1:
                df_libraryFilter = df_libraryFilter[['Library ID', 'Sample Source ID', '24h', '48h', '72h', 'Frozen', 'None', 'groupId']]
                printStr = "    |----> " + sampleID + " is found in the library " + df_libraryFilter.at[0, 'Library ID'] + " (group: " + df_libraryFilter.at[0, 'groupId'] + ")"
                runTime(printStr)
                if libraryID == df_libraryFilter.at[0, 'Library ID']:
                    printStr = "    |----> Match! input library ID: " + libraryID
                    runTime(printStr)
                else:
                    printStr = "    |----> Not Match! input library ID: " + libraryID
                    runTime(printStr)
                    printStr = "    |----> Running Stop! Please check library ID!"
                    runTime(printStr)
                    sys.exit()
            else:
                # make sure groups are in the study
                df_libraryFilter = df_libraryFilter[df_libraryFilter['groupId'].isin(libraryGroups)]
                    
                printStr = "    |----> " + sampleID + " is found in multiple libraries."
                runTime(printStr)
                for index, row in df_libraryFilter.iterrows():
                    printStr = "        |----> Library Found: " + row['Library ID'] + " (group: " + row['groupId'] + ")"
                    runTime(printStr)

                if libraryID in df_libraryFilter['Library ID'].values:
                    printStr = "    |----> Match! input library ID: " + libraryID
                    runTime(printStr)
                    df_libraryFilter = df_libraryFilter[df_libraryFilter['Library ID']==libraryID]
                    df_libraryFilter = df_libraryFilter[['Library ID', 'Sample Source ID', '24h', '48h', '72h', 'Frozen', 'None', 'groupId']]
                else:
                    printStr = "    |----> Not Match! input library ID: " + libraryID
                    runTime(printStr)
                    printStr = "    |----> Running Stop! Please check library ID!"
                    runTime(printStr)
                    sys.exit()
    except integration_curator.rest.ApiException as e:
        print("Exception when calling LibraryIntegrationApi->get_libraries_by_samples: %s\n" % e)

    runTime("3. Combining results, please waiting ...")
    library_sampleIDs = df_libraryFilter['Sample Source ID'].values.item()
    if sampleID in library_sampleIDs:
        lib_index = library_sampleIDs.index(sampleID)
        library_24h = df_libraryFilter['24h'].values.item()
        library_48h = df_libraryFilter['48h'].values.item()
        library_72h = df_libraryFilter['72h'].values.item()
        library_frozen = df_libraryFilter['Frozen'].values.item()
        library_none = df_libraryFilter['None'].values.item()
        state_list = pd.DataFrame({'24h': library_24h,
                                   '48h': library_48h,
                                   '72h': library_72h,
                                   'Frozen': library_frozen,
                                   'None': library_none})
        sample_state = state_list.iloc[lib_index][state_list.iloc[lib_index] == True].index[0]

        # query dataframe to numpy array
        querystr = "`Sample Source ID`" + "==" + "'" + sampleID + "'"
        query_state = df_sampleFilter.query(querystr)['State'][0]
        query_viability = df_sampleFilter.query(querystr)['Viability'][0]
        if type(query_state) is list:
            sample_viability = query_viability[query_state.index(sample_state)]
        else:
            sample_viability = query_viability
        printStr = "    |----> " + sampleID + '  ' + libraryID + '  ' + sample_state + '  ' + sample_viability
        runTime(printStr)
    else:
        printStr = "    |----> Error! " + sampleID + " is not in " + libraryID + "!"
        runTime(printStr)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])