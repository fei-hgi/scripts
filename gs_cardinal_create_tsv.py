#!/usr/bin/python3

################################################################
#                                                              #
#  This script is to read Excel sheets and write to .tsv file  #
#                                                              #
################################################################

################
#### ReadMe ####
################

'''
USAGE:\tgs_excel.py -e <excel file> -o <output directory> -p <output prefix>

Arguments:
\t-e, --efile           the path of excel file
\t-o, --outdir          output directory
\t-p, --outpre          output prefix

Flags:
\t-h, --help            help information
\t-v, --version         version information

Note:
\t1. need to connect mlwarehouse to extract meta information

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

HOST = "mlwh-db-ro"
PORT = "3435"
USER = "mlwh_humgen"
PASSWORD = "mlwh_humgen_is_secure"
DATABASE = "mlwarehouse"

#####################
#### subfunction ####
#####################

def versions():
    verStr = "Program:\tgs_cardinal_create_tsv.py\nVersion:\t1.0"
    print(verStr)

def usageInfo():
    versions()
    print(__doc__)

def welcomeWords():
    welWords = "Welcome to use gs_cardinal_create_tsv.py"
    print("*"*(len(welWords)+20))
    print("*"," "*(7),welWords," "*(7),"*")
    print("*"*(len(welWords)+20))

def runTime(strTmp):
    runningTime = str(datetime.now())
    runningTime = runningTime.split(".")[0]
    print(runningTime,">>>>",strTmp)

def createSQLCmd(lib_id:str):
    sqlcmd = """SELECT DISTINCT 
	                study.id_study_tmp                   as study_id,
	                study.name                           as study_name,
	                iseq_flowcell.id_pool_lims           as pool_id,
	                iseq_flowcell.id_library_lims        as library_id,
	                donors.name                          as donor_name,
	                donors.supplier_name                 as donor_supplier,
	                original_study.name                  as cohort,
                    stock_resource.labware_human_barcode as barcode
                FROM 
	                mlwarehouse.iseq_flowcell 
    	                JOIN mlwarehouse.sample                               ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp 
                        JOIN mlwarehouse.study                                ON iseq_flowcell.id_study_tmp = study.id_study_tmp 
    	                JOIN mlwarehouse.iseq_product_metrics                 ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp 
    	                JOIN mlwarehouse.iseq_run                             ON iseq_run.id_run = iseq_product_metrics.id_run 
    	                JOIN mlwarehouse.iseq_run_lane_metrics                ON iseq_run_lane_metrics.id_run = iseq_run.id_run 
    	                JOIN mlwarehouse.psd_sample_compounds_components pscc ON iseq_flowcell.id_sample_tmp = pscc.compound_id_sample_tmp 
    	                JOIN mlwarehouse.sample as donors                     ON donors.id_sample_tmp = pscc.component_id_sample_tmp 
    	                JOIN mlwarehouse.stock_resource                       ON donors.id_sample_tmp = stock_resource.id_sample_tmp 
    	                JOIN mlwarehouse.study as original_study              ON original_study.id_study_tmp = stock_resource.id_study_tmp 
                WHERE 
 	                iseq_flowcell.id_library_lims = """

    sqlcmd = sqlcmd + "\"" + lib_id + "\""

    return sqlcmd

def formatData(df_in:str, firstCol:str):
    for key, value in df_in.items():
        newkey = key.replace('#', '')
        newkey = newkey.rstrip()
        newkey = newkey.replace('  ', ' ')
        if newkey == 'nanodrop': newkey = 'Nanodrop'
        df_in.rename({key: newkey}, axis = 1, inplace = True)
        
        if newkey == 'PBMC extraction date':
            df_in[newkey] = df_in[newkey].astype('str')
            df_in[newkey] = (df_in[newkey].str.split(' ', expand = True))[0]

    df_in[firstCol] = df_in[firstCol].astype('str') 
    df_in[firstCol] = df_in[firstCol].str.strip()
 
    if firstCol == 'SAMPLE BARCODE':
        df_in['LCA PBMC Pools'] = df_in['LCA PBMC Pools'].astype('str')
        df_in['LCA PBMC Pools'] = df_in['LCA PBMC Pools'].str.strip()

    return df_in

def seprateLib(df_lib:str, list_pool:list):
    list_regex = ["^" + p + "_" for p in list_pool] 
    regex = '|'.join(list_regex)

    mask = df_lib['LCA_POOLs'].str.contains(regex)
    df_lib_filtered = df_lib[mask]

    return df_lib_filtered

def createLib(df_library:str, df_donor_state:str, df_lib_sid:str):
    df_library_state = seprateLib(df_library, set(list(df_donor_state['LCA PBMC Pools'])))
    df_lib_sid_state = df_lib_sid[df_lib_sid['Sample Source ID'].isin(list(df_donor_state['Sample Source ID']))]
    for index, row in df_library_state.iterrows():
        df_tmp = df_lib_sid_state[df_lib_sid_state['Library ID']==row['Library ID']]
        if not df_tmp.empty:
            df_library_state.at[index, 'Sample Source ID'] = '|'.join(list(df_tmp['Sample Source ID']))

    df_library_state = df_library_state[df_library_state['Sample Source ID'].notna()]    
    return df_library_state

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
#import pyodbc
import mysql.connector
import pandas as pd
import re

def main(argvs):
    #------------#
    # parameters #
    #------------#
    excelFile = ""
    outputPrefix = ""

    try:
        opts, args = getopt.getopt(argvs,"vhe:o:p:",["version","help","efile=","outdir=", "outpre="])
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
        elif opt in ("-e", "--efile"):
            excelFile = arg
        elif opt in ("-o", "--outdir"):
            outputDir = arg
        elif opt in ("-p", "--outpre"):
            outputPrefix = arg
        else:
            assert False, "unhandled option"

    if excelFile == "": sys.exit("Please use the correct arguments, option -f is missing")
    if outputDir == "": sys.exit("Please use the correct arguments, option -o is missing")
    if outputPrefix == "": sys.exit("Please use the correct arguments, option -p is missing")

    #------------#
    # processing #
    #------------#
    welcomeWords()

    ####
    runTime("1. Reading Excel file, please waiting ...")
    
    xl = pd.ExcelFile(excelFile)
    printstr = "   Found sheets: " + ', '.join(str(s) for s in xl.sheet_names)
    runTime(printstr)

    # parse sheets
    df_pbmc = xl.parse("LCA_PBMC")
    df_cdna = xl.parse("LCA_CDNA")
    df_pcrxp = xl.parse("LCA_PCRXP")

    # remove rows with NAs in key columns
    df_pbmc.dropna(subset=['SAMPLE BARCODE'], inplace = True)
    df_cdna.dropna(subset=['LCA_POOLs'], inplace = True)
    df_pcrxp.dropna(subset = ['LCA Connect PCRXP'], inplace = True)

    ####
    runTime("2. Creating donor tsv for genestack table, please waiting ...")

    df_donor = pd.DataFrame(
        columns = ['Sample Source',            'Sample Source ID', 'Human Barcode', 
                   'SequenceScape Donor Name', 'Cohort',           'PBMC extraction date',
                   'State',                    'LCA_BLOOD_ARRAY',  'LCA_PBMC',
                   'Cellaca ID',               'Live cells',       'Viability',
                   'BrightField',              'RBC Ratio',        'Viability Pass',
                   'Conc Pass',                'RBC Pass',         'Error Log',
                   'LCA PBMC Pools',           'Antibody batch',   'RapidSphere Beads',
                   'Note'])

    # correct default column names in df_pbmc
    df_pbmc = formatData(df_pbmc, 'SAMPLE BARCODE')

    for key, value in df_donor.items():
        if key in df_pbmc.columns:
            df_donor[key] = df_pbmc[key]
        else:
            if key == "Sample Source":
                df_donor[key] = ['Cardinal'] * df_pbmc.shape[0]
            elif key == "Sample Source ID":
                df_donor[key] = df_pbmc['SAMPLE BARCODE']

    for index, row in df_donor.iterrows():
        if re.search("^300", str(row['Sample Source ID'])) :
            df_donor.at[index, 'Sample Source ID'] = str("00") + str(row['Sample Source ID'])

    # replace nan with space, otherwise hard to create genestack tsv
    df_donor = df_donor.fillna(' ')

    ####
    runTime("3. Reading library and pool information from sheets, please waiting ...")
    
    df_library = pd.DataFrame(
        columns = ['Library ID',                     'Sample Source ID',                  'Human Barcode', 
                   '24h',                            '48h',                               '72h',
                   'Frozen',                         'None',                              'LCA_POOLs',
                   'GEM check',                      'LCA_cDNA',                          'cDNA check',
                   'cDNA banked',                    'cDNA tube ID',                      'GEMS batch',
                   'Pool Error Log',                 'Tag ID',                            'Connect complete run',
                   'Library Error Log',              'Library tube ID',                   'Library rack ID',
                   'Stock Multiplexed Library Tube', 'Multiplexed Barcode',               'Library Concentration (pM)',
                   'Number of live cells',           'Number of predicted Capture Cells', 'MultiplexedLibraryTube',
                   'Nanodrop',                       'Note'])
    
    # correct default column names in df_cdna
    df_cdna = formatData(df_cdna, 'LCA_POOLs')

    for key, value in df_library.items():
        if key in df_cdna.columns:
            df_library[key] = df_cdna[key]
        else:
            if key == "Pool Error Log":
                df_library[key] = df_cdna['Error Log']

    # correct default column names in df_pcrxp
    df_pcrxp = formatData(df_pcrxp, 'LCA Connect PCRXP')

    for key, value in df_library.items():
        if key in df_pcrxp.columns:
            df_library[key] = df_pcrxp[key]
        else:
            if key == "Library ID":
                df_library[key] = df_pcrxp['LCA Connect PCRXP']
            elif key == "Library Error Log":
                df_library[key] = df_pcrxp['Error Log']

    df_library['Library ID'] = df_library['Library ID'].str.replace('_', ':')
    df_library['Library ID'] = df_library['Library ID'] + str(1)

    df_library = df_library.fillna(' ')

    #####
    runTime("4. Extracting meta information from mlwarehouse, please waiting ...")
    
    mydb = mysql.connector.connect(host = HOST,
                                   port = PORT,
                                   user = USER,
                                   password = PASSWORD,
                                   database = DATABASE)
 
    for index, row in df_library.iterrows():
        cursor = mydb.cursor()
        cursor.execute(createSQLCmd(row['Library ID']))
        colnames = [ x[0] for x in cursor.description ]
        fetchrows = cursor.fetchall()

        if cursor.rowcount == 0:
            printstr = "   !!!WARNING!!! No meta information fetched for: " + row['Library ID']
            runTime(printstr)
            printstr = "   !!!WARNING!!! " + row['Library ID'] + " may failed"
            runTime(printstr)
        else:
            df_ss = pd.DataFrame(fetchrows, columns = colnames)
            #df_ss.drop_duplicates(keep = 'first', inplace = True)

            dict_id2name = df_ss.set_index('donor_supplier').to_dict()['donor_name']
            dict_id2cohort = df_ss.set_index('donor_supplier').to_dict()['cohort']
            dict_id2barcode = df_ss.set_index('donor_supplier').to_dict()['barcode']
            dict_barcode2id = df_ss.set_index('barcode').to_dict()['donor_supplier']

            for dindex, drow in df_donor.iterrows():
                if drow['Sample Source ID'] in dict_id2name :
                    df_donor.at[dindex, 'SequenceScape Donor Name'] = dict_id2name[drow['Sample Source ID']]
                    df_donor.at[dindex, 'Cohort'] = dict_id2cohort[drow['Sample Source ID']]
                    df_donor.at[dindex, 'Human Barcode'] = dict_id2barcode[drow['Sample Source ID']]
                elif drow['Sample Source ID'] in dict_barcode2id:
                    df_donor.at[dindex, 'Human Barcode'] = drow['Sample Source ID']
                    df_donor.at[dindex, 'Sample Source ID'] = dict_barcode2id[drow['Sample Source ID']]
                    df_donor.at[dindex, 'SequenceScape Donor Name'] = dict_id2name[df_donor.at[dindex, 'Sample Source ID']]
                    df_donor.at[dindex, 'Cohort'] = dict_id2cohort[df_donor.at[dindex, 'Sample Source ID']]

            df_library.at[index, 'Sample Source ID'] = '|'.join(list(df_ss['donor_supplier']))
            df_library.at[index, 'Human Barcode'] = '|'.join(list(df_ss['barcode']))

            for sindex, srow in df_ss.iterrows():
                cond1 = df_donor['Sample Source ID'] == srow['donor_supplier']
                # using df_library row
                pool_id = row['LCA_POOLs'].split("_")[0]
                cond2 = df_donor['LCA PBMC Pools'].eq(pool_id)
                list_in = list(df_donor.loc[cond1 & cond2, 'State'])
   
                # check those IDs using barcodes
                if not list_in:
                    barcode_id = dict_id2barcode[srow['donor_supplier']]
                    cond1 = df_donor['Sample Source ID'] == barcode_id
                    # using df_library row
                    pool_id = row['LCA_POOLs'].split("_")[0]
                    cond2 = df_donor['LCA PBMC Pools'].eq(pool_id)
                    list_in = list(df_donor.loc[cond1 & cond2, 'State'])

                check_24h = "No"
                check_48h = "No"
                check_72h = "No"
                check_frozen = "No"
                check_none = "No"
                for lstate in list_in:
                    if lstate == "24h":      check_24h = "Yes"
                    elif lstate == "48h":    check_48h = "Yes" 
                    elif lstate == "72h":    check_72h = "Yes" 
                    elif lstate == "frozen": check_frozen = "Yes"
                    else:                    check_none = "Yes"

                # first element is ' '
                if str(df_library.at[index, '24h']) == " ":
                    df_library.at[index, '24h'] = check_24h
                    df_library.at[index, '48h'] = check_48h
                    df_library.at[index, '72h'] = check_72h
                    df_library.at[index, 'Frozen'] = check_frozen
                    df_library.at[index, 'None'] = check_none
                else:
                    df_library.at[index, '24h'] = str(df_library.at[index, '24h']) + '|' + check_24h
                    df_library.at[index, '48h'] = str(df_library.at[index, '48h']) + '|' + check_48h
                    df_library.at[index, '72h'] = str(df_library.at[index, '72h']) + '|' + check_72h
                    df_library.at[index, 'Frozen'] = str(df_library.at[index, 'Frozen']) + '|' + check_frozen
                    df_library.at[index, 'None'] = str(df_library.at[index, 'None']) + '|' + check_none

        cursor.close()
    mydb.close()

    ####
    runTime("5. Removing duplicated Sample Source IDs and refining table, please waiting ...")

    #df_library_new = df_library.dropna(subset=['Sample Source ID'])
    df_library.loc[df_library['Sample Source ID']==' ', ['Sample Source ID', 'Note']] = ['Not Found', 'This library failed, and would re-do in the upcoming weeks']

    df_donor_dup = df_donor[df_donor.duplicated('Sample Source ID', keep = False)]
    df_donor_rmdup = df_donor.drop_duplicates('Sample Source ID', keep = False)

    df_donor_dup2uni = pd.DataFrame().reindex_like(df_donor_dup).dropna()
    idx = 0
    dupids = set(df_donor_dup['Sample Source ID'])
    for id in dupids:
        df_donor_dup_tmp = df_donor_dup[df_donor_dup['Sample Source ID'] == id]
        for key, value in df_donor_dup_tmp.items():
            if key in ['Sample Source', 'Sample Source ID', 'Human Barcode', 'SequenceScape Donor Name', 'Cohort', 'Note']:
                df_donor_dup2uni.at[idx, key] = value.iloc[0]
            else:
                df_donor_dup2uni.at[idx, key] = '|'.join(str(s) for s in list(value))
        idx += 1
    
    df_donor_new = pd.concat([df_donor_rmdup, df_donor_dup2uni]).reset_index(drop=True)

    ####
    runTime("6. Check donors without libraries in meta sheet and add them in, please waiting ...")
    for index, row in df_donor_new.iterrows():
        if row['SequenceScape Donor Name'] == ' ':
            df_donor_new.at[index, 'SequenceScape Donor Name'] = 'Refer to Note'
            df_donor_new.at[index, 'Human Barcode'] = 'Refer to Note'
            df_donor_new.at[index, 'Cohort'] = 'Refer to Note'
            df_donor_new.at[index, 'Note'] = 'This sample failed, and would re-do in the upcoming weeks'

    #--------------#
    # Output Files #
    #--------------#

    ####
    runTime("7. Creating output files, please waiting ...")

    donorDir = outputDir + "/donors"
    libraryDir = outputDir + "/libraries"

    if not os.path.exists(donorDir): os.makedirs(donorDir)
    if not os.path.exists(libraryDir): os.makedirs(libraryDir)

    outfile = donorDir + "/" + outputPrefix + ".tsv"
    df_donor_new.to_csv(outfile, sep = "\t", index = False)
    printstr = "   Donor tsv file: " + outfile
    runTime(printstr)

    if not df_library.empty:
        outfile = libraryDir + "/" + outputPrefix + ".tsv"
        df_library.to_csv(outfile, sep = "\t", index = False)
        printstr = "   Library tsv file: " + outfile
        runTime(printstr)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])