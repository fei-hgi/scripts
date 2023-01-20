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
USAGE:\tgs_excel.py -e <excel file> -o <output prefix>

Arguments:
\t-e, --efile           the path of excel file
\t-o, --output          output file prefix

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
    verStr = "Program:\tgs_excel.py\nVersion:\t1.0"
    print(verStr)

def usageInfo():
    versions()
    print(__doc__)

def welcomeWords():
    welWords = "Welcome to use gs_excel.py"
    print("*"*(len(welWords)+20))
    print("*"," "*(7),welWords," "*(7),"*")
    print("*"*(len(welWords)+20))

def runTime(strTmp):
    runningTime = str(datetime.now())
    runningTime = runningTime.split(".")[0]
    print(runningTime,">>>>",strTmp)

def createSQLCmd(lib_id:str):
    sqlcmd = """SELECT DISTINCT 
	                study.id_study_tmp            as study_id,
	                study.name                    as study_name,
	                iseq_flowcell.id_pool_lims    as pool_id,
	                iseq_flowcell.id_library_lims as library_id,
	                donors.name                   as donor_name,
	                donors.supplier_name          as donor_supplier,
	                sample.name                   as sample_name,
	                original_study.name           as cohort
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
        opts, args = getopt.getopt(argvs,"vhe:o:",["version","help","efile=","outputprefix="])
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
        elif opt in ("-o", "--outputprefix"):
            outputPrefix = arg
        else:
            assert False, "unhandled option"

    if excelFile == "": sys.exit("Please use the correct arguments, option -f missing")
    if outputPrefix == "": sys.exit("Please use the correct arguments, option -o missing")

    #------------#
    # processing #
    #------------#
    welcomeWords()
    runTime("1. Reading Excel file, please waiting ...")
    
    xl = pd.ExcelFile(excelFile)
    printstr = "   Found sheets: " + ', '.join(str(s) for s in xl.sheet_names)
    runTime(printstr)

    df_pbmc = xl.parse("LCA_PBMC")
    df_cdna = xl.parse("LCA_CDNA")
    df_pcrxp = xl.parse("LCA_PCRXP")

    # 1. create the donor table template
    runTime("2. Creating donor tsv for genestack table, please waiting ...")

    df_donor = pd.DataFrame(
        columns = ['Sample Source',   'Sample Source ID',     'SequenceScape Donor Name', 
                   'Cohort',          'PBMC extraction date', 'State',
                   'LCA_BLOOD_ARRAY', 'LCA_PBMC',             'Cellaca ID',
                   'Live cells',      'Viability',            'BrightField',
                   'RBC Ratio',       'Viability Pass',       'Conc Pass',    
                   'RBC Pass',        'Error Log',            'LCA PBMC Pools', 
                   'Antibody batch',  'RapidSphere Beads',    'Note'])

    for (key, value) in df_donor.items():
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

    # 2. create the library table template
    runTime("3. Reading library and pool information from sheets, please waiting ...")
    
    df_library = pd.DataFrame(
        columns = ['Library ID',                     'Sample Source ID',                  'LCA_POOLs',               
                   'GEM check',                      'LCA_cDNA',                          'cDNA check',                 
                   'cDNA banked',                    'cDNA tube ID',                      'GEMS batch',             
                   'Pool Error Log',                 'Tag ID',                            'Connect complete run',       
                   'Library Error Log',              'Library tube ID',                   'Library rack ID',     
                   'Stock Multiplexed Library Tube', 'Multiplexed Barcode',               'Library Concentration (pM)',
                   'Number of live cells',           'Number of predicted Capture Cells', 'MultiplexedLibraryTube',
                   'Note'])
    
    for (key, value) in df_library.items():
        if key in df_cdna.columns:
            df_library[key] = df_cdna[key]
        else:
            if key == "GEMS batch":
                df_library[key] = df_cdna['GEMS batch #']
            elif key == "Pool Error Log":
                df_library[key] = df_cdna['Error Log']

    for (key, value) in df_library.items():
        if key in df_pcrxp.columns:
            df_library[key] = df_pcrxp[key]
        else:
            if key == "Library ID":
                df_library[key] = df_pcrxp['LCA Connect PCRXP']
            elif key == "Library Error Log":
                df_library[key] = df_pcrxp['Error Log']

    df_library['Library ID'] = df_library['Library ID'].str.replace('_', ':')
    df_library['Library ID'] = df_library['Library ID'] + str(1)

    runTime("4. Extracting meta information from mlwarehouse, please waiting ...")
    
    df_lib_sid = pd.DataFrame(columns = ['Library ID', 'Sample Source ID'])

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
            printstr = "   WARNING!!! No meta information fetched for: " + row['Library ID']
            runTime(printstr)
        else:
            df_ss = pd.DataFrame(fetchrows, columns = colnames)
            dict_id2name = df_ss.set_index('donor_supplier').to_dict()['donor_name']
            dict_id2cohort = df_ss.set_index('donor_supplier').to_dict()['cohort']

            for dindex, drow in df_donor.iterrows():
                if drow['Sample Source ID'] in dict_id2name :
                    df_donor.at[dindex, 'SequenceScape Donor Name'] = dict_id2name[drow['Sample Source ID']]
                    df_donor.at[dindex, 'Cohort'] = dict_id2cohort[drow['Sample Source ID']]

            df_tmp = pd.DataFrame(columns = ['Library ID', 'Sample Source ID'])
            df_tmp['Sample Source ID'] = df_ss['donor_supplier']
            df_tmp['Library ID'] = row['Library ID']
            df_lib_sid = pd.concat([df_lib_sid,df_tmp], ignore_index = True, sort = False)

            #df_library.at[index, 'Sample Source ID'] = '|'.join(list(df_ss['donor_supplier']))

        cursor.close()
    mydb.close()

    runTime("5. Seprating libraries into subsets ...")

    df_donor_24h = df_donor[df_donor['State'] == '24h']
    if not df_donor_24h.empty: 
        df_library_24h = createLib(df_library, df_donor_24h, df_lib_sid)

    df_donor_48h = df_donor[df_donor['State'] == '48h']
    if not df_donor_48h.empty: 
        df_library_48h = createLib(df_library, df_donor_48h, df_lib_sid)

    df_donor_frozen = df_donor[df_donor['State'] == 'frozen']
    if not df_donor_frozen.empty: 
        df_library_frozen = createLib(df_library, df_donor_frozen, df_lib_sid)

    df_donor_nan = df_donor[df_donor['State'].isna()]
    if not df_donor_nan.empty: 
        df_library_nan = createLib(df_library, df_donor_nan, df_lib_sid)


    #--------------#
    # Output Files #
    #--------------#

    runTime("6. Creating output files, please waiting ...")

    outfile = outputPrefix + "_donors.tsv"
    df_donor.to_csv(outfile, sep = "\t", index = False)
    printstr = "   Donor tsv file: " + outfile
    runTime(printstr)

    if not df_donor_24h.empty:
        outfile = "24h.tsv"
        df_library_24h.to_csv(outfile, sep = "\t", index = False)
        printstr = "   Library tsv file: " + outfile
        runTime(printstr)
    
    if not df_donor_48h.empty:
        outfile = "48h.tsv"
        df_library_48h.to_csv(outfile, sep = "\t", index = False)
        printstr = "   Library tsv file: " + outfile
        runTime(printstr)

    if not df_donor_frozen.empty:
        outfile = "frozen.tsv"
        df_library_frozen.to_csv(outfile, sep = "\t", index = False)
        printstr = "   Library tsv file: " + outfile
        runTime(printstr)

    if not df_donor_nan.empty:
        outfile = "nan.tsv"
        df_library_nan.to_csv(outfile, sep = "\t", index = False)
        printstr = "   Library tsv file: " + outfile
        runTime(printstr)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])