#!/usr/bin/env python

"""
Set of simple function to extract most-likely calibration files for a given nite
F.Menanteau Apr 2015.
"""

import querylibs  as ql
import dbfuncs    as dbf
from datetime import datetime
import os,sys


def getNITE(cal,caltype,key='NITE'):
    return cal[caltype][key][0]

def getFNAME(cal,caltype,key='FILENAME'):
    return cal[caltype][key][0]

def getLAST(cal,caltype,key='FILENAME'):
    return sorted(cal[caltype][key])[-1] # to get the latest

def getBPM_info(cal,caltype='cal_bpm'):

    # Check for consitent numbers
    N = len(cal[caltype]['FILENAME'])
    reqnum = 1e6
    for k in range(N):
        tmp_reqnum = cal[caltype]['FILENAME'][k][22:26]
        attnum = cal[caltype]['FILENAME'][k][27:29]
        nite   = cal[caltype]['NITE'][k]
        reqnum = min(reqnum, int(tmp_reqnum))
    print cal[caltype]['FILENAME']
    return nite,reqnum,attnum

def construct_wcl_block(cal,nite,verb=False,safeBPM=False):
    
    if verb: print "# Formatting wcl-block"
    blck = ''
    blck = blck +  "# --- wcl calibration configuration inputs for nite: %s  ---- \n" % nite
    blck = blck +  "xtalkcoefffile  = %s\n" % getFNAME(cal,'cal_xtalk')
    blck = blck +  "hupdatefile     = %s\n" % getLAST(cal,'%_update.%') # to get the latest
    blck = blck +  "lintablenite    = 20130624\n" # This iblckrmation is not in the DB
    blck = blck +  "lintablefile    = %s\n" % os.path.splitext(getFNAME(cal,'cal_lintable'))[0]
    if safeBPM:
        if verb: print "# Using safe BPM files"
        blck = blck +  "bpmnite         = 20140901t0928\n"
        blck = blck +  "bpmreq          = 1190\n"
        blck = blck +  "bpmatt          = 1\n"
    else:
        bpmnite,bpmreq,bpmatt = getBPM_info(cal)
        blck = blck +  "bpmnite         = %s\n" % bpmnite
        blck = blck +  "bpmreq          = %s\n" % bpmreq
        blck = blck +  "bpmatt          = %s\n" % int(bpmatt)
    blck = blck +  "fringecornite   = %s\n" % getNITE(cal,'cal_fringecor')
    blck = blck +  "illumcornite    = %s\n" % getNITE(cal,'cal_illumcor')
    blck = blck +  "pupilnite       = %s\n" % getNITE(cal,'cal_pupil')
    blck = blck +  "photflatcornite = null\n"     # This iblckrmation is not in the DB
    blck = blck +  "precalnite      = %s\n" % getNITE(cal,'cal_biascor')
    blck = blck +  "precalrun       = r%04dp%02d\n"  % (cal['cal_biascor']['REQNUM'][0],cal['cal_biascor']['ATTNUM'][0])
    # -------------------------------------------
    # Hard-coding this ones for now
    #blck = blck +  "precalnite      = 20121207\n" 
    #blck = blck +  "precalrun       = r1415p02\n" 
    # --------------------------------------------
    blck = blck +  "scampupdatenite = %s\n" % getLAST(cal,'\S+_decam_pvmodel_\d+.ahead').split('.ahead')[0][-8:]
    blck = blck +  "scampconfigfile = %s\n" % getLAST(cal,'%_default.scamp.%')
    blck = blck +  "etcnite         = %s\n" % cal['%_sex.config']['NITE'][0]
    blck = blck +  "# ---------------------------------------------------"
    return blck

def print_wcl_calconfig(cal,nite):

    blck = construct_wcl_block(cal,nite)
    print blck
    return

def get_cals_info(**kwargs):
    
    nite         = kwargs.get('nite')
    db_section   = kwargs.get('db_section')
    archive_name = kwargs.get('archive_name')
    verb         = kwargs.get('verb',False)
    
    today = datetime.today().strftime('%Y%m%d')
    
    # Get the location in the new database section (root_archive)
    dbh          = dbf.get_dbh(db_section=db_section,verb=verb)
    #root_archive = dbf.get_root_archive(dbh, archive_name=archive_name,verb=verb)

    # Get all calibrations
    # NOTE: for cal_photflatcor we always use 'null' which is a bunch of one
    cal_copied = ['cal_pupil','cal_fringecor','cal_illumcor'] # 'cal_photflatcor']
    cal_run    = ['cal_biascor','cal_dflatcor']

    cal_info = {}
    for cal_type in cal_run:
        if verb: print "# Geting: %s" % cal_type
        cal_info[cal_type] = ql.get_cal_run(dbh,caltype=cal_type,nite=nite,verb=verb)

    for cal_type in cal_copied:
        if verb: print "# Geting: %s" % cal_type
        cal_info[cal_type] = ql.get_cal_copied(dbh,caltype=cal_type,nite=nite,verb=verb)
        
    file_types = ['cal_xtalk','cal_lintable','cal_bpm']
    for file_type in file_types:
        if verb: print "# Geting: %s" % file_type
        cal_info[file_type] = ql.get_file(dbh,filetype=file_type,nite=nite,verb=verb)

    config_types = ['%_sex.config','%_update.%','%_default.scamp.%','\S+_decam_pvmodel_\d+.ahead']
    for config_type in config_types:
        if verb: print "# Geting: %s" % config_type
        cal_info[config_type] = ql.get_config(dbh,like=config_type,nite=today,verb=verb)

    return cal_info
    


