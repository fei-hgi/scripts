#!/usr/bin/python3

#######################################################################
#                                                                     #
#  This script is to read Excel sheets and check libraries and donors #
#                                                                     #
#######################################################################

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
USAGE:\t{script_name} -e <excel file> -o <output directory> -p <output prefix>

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

########################
#### process module ####
########################

import pandas as pd
import re

#######################
#### main function ####
#######################

def main(argvs):
    #------------#
    # parameters #
    #------------#
    excelFile = ""
    outputPrefix = ""

    try:
        opts, args = getopt.getopt(argvs,"vhe:o:p:",["version","help","efile=","outdir=", "outpre="])
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
    runTime("1. Reading Excel file, please wait ...")
    
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
    runTime("2. Creating donor tsv for genestack table, please wait ...")

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
    runTime("3. Reading library and pool information from sheets, please wait ...")
    
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
   
    #--------------#
    # Output Files #
    #--------------#

    ####
    runTime("4. Creating output files, please wait ...")

    df_donor_out = df_donor[['Sample Source ID', 'Viability']]
    df_library_out = df_library[['Library ID', 'GEMS batch']]

    donorDir = outputDir + "/donors"
    libraryDir = outputDir + "/libraries"

    if not os.path.exists(donorDir): os.makedirs(donorDir)
    if not os.path.exists(libraryDir): os.makedirs(libraryDir)

    outfile = donorDir + "/" + outputPrefix + ".tsv"
    df_donor_out.to_csv(outfile, sep = "\t", index = False)
    printstr = "   Donor tsv file: " + outfile
    runTime(printstr)

    if not df_library_out.empty:
        outfile = libraryDir + "/" + outputPrefix + ".tsv"
        df_library_out.to_csv(outfile, sep = "\t", index = False)
        printstr = "   Library tsv file: " + outfile
        runTime(printstr)

#####################
#### program run ####
#####################

if __name__ == "__main__":
  main(sys.argv[1:])