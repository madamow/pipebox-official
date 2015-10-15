import despyastro 
import numpy
import numpy.lib.recfunctions as recfuncs 

QUERY_CAL_COPIED = """
   select distinct minnite, maxnite, nite, filename, filetype
     from calibration where filetype='{caltype}'"""

QUERY_CAL_RUN_OLD = """
   select distinct c.minnite, c.maxnite, c.nite, wgb.reqnum, wgb.attnum, c.filename, c.filetype, pfw.DATA_STATE
   from wgb, calibration c, PFW_ATTEMPT pfw
   where c.filetype='{caltype}'
         and wgb.filename=c.filename
         and pfw.DATA_STATE='ACTIVE'
   """

# Using supercal/precal tags for safety
QUERY_CAL_RUN = """
  select distinct c.minnite, c.maxnite, c.nite, wgb.reqnum, wgb.attnum, c.filename, c.filetype, pfw.DATA_STATE, o.TAG
  from wgb, calibration c, PFW_ATTEMPT pfw, OPS_PROCTAG o
  where c.filetype='{caltype}'
        and wgb.filename=c.filename
        and pfw.DATA_STATE='ACTIVE'
        and o.TAG='Y2T_PRECAL'
        and wgb.attnum=o.attnum
        and wgb.reqnum=o.reqnum
   """

#QUERY_CAL_RUN = """
#   select distinct c.minnite, c.maxnite, c.nite, wgb.reqnum, wgb.unitname, wgb.attnum, c.filename, c.filetype, pfw.DATA_STATE
#   from wgb, calibration c, PFW_ATTEMPT pfw, task t, and OPS_PROCTAG o
#   where c.filetype='cal_dflatcor'
#         and wgb.filename=c.filename
#         and wgb.reqnum=pfw.reqnum
#         and wgb.unitname=pfw.unitname
#         and wgb.attnum=pfw.attnum
#         and t.id = pfw.task_id
#         and t.status=0
#         """

QUERY_FILE = """
    select distinct filename  from genfile
    where filetype='{filetype}'"""

QUERY_CONFIG = """
    select distinct filename from genfile
    where filetype='config' and filename like '{like}'"""

QUERY_CONFIG_REGEXP = """
    select distinct filename from genfile
    where filetype='config' and
    regexp_like(filename, '{regexp}')"""


def get_cal_run(dbh,caltype,nite=False,verb=False):
    query = QUERY_CAL_RUN.format(caltype=caltype)
    calinfo = despyastro.genutil.query2rec(query,dbh)
    if calinfo['NITE'][0] is None:
        if verb: print "# NITE is NoneType --> using MAXNITE instead"
        calinfo['NITE'] = calinfo['MAXNITE']

    # We get the info for the nearest nite only
    if nite: 
        daylist = calinfo['NITE'].tolist()
        nearest_nite = nearest_date(daylist,nite,verb)
        return calinfo[ calinfo['NITE'] == nearest_nite ]
    return calinfo

def get_cal_copied(dbh,caltype,nite=False,verb=False):

    query = QUERY_CAL_COPIED.format(caltype=caltype)
    calinfo = despyastro.genutil.query2rec(query,dbh)
    calinfo['NITE'] =  numpy.where(calinfo['NITE'],calinfo['MAXNITE'],calinfo['NITE'])
    
    # Hack for cal_types without dates
    if caltype in ['cal_pupil','cal_fringecor','cal_illumcor']:
        for k in range(len(calinfo['FILENAME'])):
            if calinfo['FILENAME'][k][0:8].isdigit():
                calinfo['NITE'][k] = calinfo['FILENAME'][k][0:8]
                if caltype == 'cal_fringecor':
                    calinfo['NITE'][k] = (calinfo['FILENAME'][k]).split('_fringecor_')[0]
            else:
                calinfo['NITE'][k] = '19690101'

    # We get the info for the nearest nite only
    if nite: 
        daylist = calinfo['NITE'].tolist()
        nearest_nite = nearest_date(daylist,nite,verb)
        return calinfo[ calinfo['NITE'] == nearest_nite ]

    return calinfo

def get_file(dbh,filetype,nite=False,verb=False):
    query = QUERY_FILE.format(filetype=filetype)
    fileinfo = despyastro.genutil.query2rec(query,dbh)

    # We need to get the closest night
    if filetype == 'cal_bpm' and nite:
        dates   = []
        reqnums = []
        attnums = []
        for k in range(len(fileinfo['FILENAME'])):
            if fileinfo['FILENAME'][k][0:8].isdigit():
                #date   = fileinfo['FILENAME'][k][0:8]
                date = '19690101'
            elif fileinfo['FILENAME'][k][3:11].isdigit():
                #date   = fileinfo['FILENAME'][k][3:11]
                date   = fileinfo['FILENAME'][k][3:16]
                #reqnum = fileinfo['FILENAME'][k][22:26]
                #attnum = fileinfo['FILENAME'][k][27:29]
            else:
                date = '19690101'

            dates.append(date)
            
        nearest_nite = nearest_date(dates,nite,verb)
        # Add and extra field=DATE to the recarray
        extra = numpy.rec.fromrecords(zip(dates),names=['NITE'])
        fileinfo = fixNone(fileinfo)
        fileinfo = recfuncs.merge_arrays([fileinfo,extra],flatten=True,asrecarray=True)
        return fileinfo[ fileinfo['NITE'] == nearest_nite ]

    return fileinfo


def get_config(dbh,like,nite=False,verb=False):

    if like == '\S+_decam_pvmodel_\d+.ahead':
        query = QUERY_CONFIG_REGEXP.format(regexp=like) 
    else:
        query = QUERY_CONFIG.format(like=like)
    info = despyastro.genutil.query2rec(query,dbh)

    if nite:
        dates = []
        for k in range(len(info['FILENAME'])):
            if info['FILENAME'][k][0:8].isdigit():
                date = info['FILENAME'][k][0:8]
            elif info['FILENAME'][k][3:11].isdigit():
                date = info['FILENAME'][k][3:11]
            else:
                date = '19690101'
            dates.append(date)
        nearest_nite = nearest_date(dates,nite,verb)
        # Add and extra field=DATE to the recarray
        extra = numpy.rec.fromrecords(zip(dates),names='NITE')
        info = fixNone(info)
        info = recfuncs.merge_arrays([info,extra],flatten=True,asrecarray=True)
        return info[ info['NITE'] == nearest_nite ]

    return info
    

def nearest_date(daylist,day,verb=False):

    from datetime import timedelta, datetime

    # Format date "YEAR MONTH DAY" and construct the the datetime objects
    DAY  =   datetime.strptime(date_format(day), "%Y %m %d")
    DAYS = [ datetime.strptime(date_format(d[0:8]), "%Y %m %d") for d in daylist ]

    # Compute the absolute distances in time
    distances = [ abs( x-DAY) for x in DAYS]
    # In case we want the nearest time
    # nearest   = sorted (DAYS, key=lambda x: abs (x-DAY))[0]

    # Return the sorted indexes and the one we want
    id0 = sorted( range(len(distances)), key=lambda x:distances[x])[0]
    if verb: print "# Closest day to %s is %s at %s away" % (day, daylist[id0], distances[id0])
    return daylist[id0]

def date_format(day,datefmt="{year} {month} {day}"):
    return datefmt.format(year=day[0:4],month=day[4:6],day=day[6:8])
    
def fixNone(recarray):
    """ Quick fix to change None types from O --> S, into strings"""
    dt = recarray.dtype.descr
    for k in range(len(dt)):
        # Fix the NONE types
        if dt[k][1] == '|O8':
            dt[k] = (dt[k][0],'|S08')
    recarray = recarray.astype(dt)
    return recarray

