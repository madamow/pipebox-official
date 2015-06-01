#!/usr/bin/env python

import os,sys
from despydb import desdbi
from se_calib import calib_info as cals
from datetime import datetime
from PipeBox import replace_fh

def get_expnum_info(expnum,db_section='db-destest'):

    dbh = desdbi.DesDbi(section=db_section)
    cur = dbh.cursor()
    QUERY = '''SELECT nite,band from exposure where expnum={expnum}'''
    cur.execute(QUERY.format(expnum=expnum))
    nite, band = cur.fetchone()
    return nite, band


def cmdline():

    import argparse
    parser = argparse.ArgumentParser(description="Creates the submit wcl for a EXPNUM\nwrite_firstcut_wcl_exposure.py 229686 1124")
    # The positional arguments
    parser.add_argument("expnum", action="store",default=None,
                        help="Exposure number")
    parser.add_argument("reqnum", action="store",default=None,
                        help="Request number")
    parser.add_argument("--db_section", action="store", default='db-destest',
                        choices=['db-desoper','db-destest'],
                        help="DB Section to query")
    parser.add_argument("--archive_name", default=None,   
                        help="Archive name (i.e. prodbeta or desar2home)")
    parser.add_argument("--schema", default=None,   
                        help="Schema name (i.e. prodbeta or prod)")
    parser.add_argument("--http_section", default=None,   
                        help="DES Services http-section  (i.e. file-http-prodbeta)")
    parser.add_argument("--target_site", action="store", default='fermigrid-sl6',
                        help="Compute Target Site")
    parser.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit")
    parser.add_argument("--labels", action="store", default='me-tests',
                        help="Coma-separated labels")
    parser.add_argument("--eups_product", action="store", default='Y2Nstack',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--eups_version", action="store", default='1.0.6+1',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--campaign", action="store", default='Y2T',
                        help="Name of the campaign")
    parser.add_argument("--project", action="store", default='ACT',
                        help="Name of the project ie. ACT/FM/etc")
    parser.add_argument("--libname", action="store", default='Y2N',
                        help="Name of the wcl library to use")
    parser.add_argument("--verb", action="store_true", default=False,
                        help="Turn on verbose")
    args = parser.parse_args()

    # Update depending on the db_section
    if not args.archive_name:
        if args.db_section == 'db-desoper': args.archive_name='desar2home'
        if args.db_section == 'db-destest': args.archive_name='prodbeta'

    if not args.schema:
        if args.db_section == 'db-desoper': args.schema='PROD'
        if args.db_section == 'db-destest': args.schema='PRODBETA'

    if not args.http_section:
        if args.db_section == 'db-desoper': args.http_section='file-http-desar2home' # CHECK!!!
        if args.db_section == 'db-destest': args.http_section='file-http-prodbeta'

    return args


def write_wcl(EXPNUM,args):

    # Get NITE and BAND for expnum
    NITE, BAND   = get_expnum_info(EXPNUM,args.db_section)
    LIBNAME = args.libname
    template = os.path.join(os.environ['PIPEBOX_DIR'],"libwcl/%s/submitwcl/firstcut_template.des" % LIBNAME)

    # Fring Case
    if BAND in ['z','Y']:
        FRINGE_CASE = 'fringe'
    else:
        FRINGE_CASE = 'nofringe'

    # Get the right names depending on the db_section
    #if args.db_section == 'db-destest':
    #    SCHEMA       = 'PRODBETA'
    #    HTTP_SECTION = 'file-http-prodbeta'
    #elif args.db_section == 'db-desoper':
    #    SCHEMA       = 'PROD'
    #    HTTP_SECTION = 'file-http-desar2home' # CHECK!!!
    #else:
    #    exit("ERROR: No schema defined for section: %s" % args.db_section)

    # Open template file and replace file-handle
    newlib = os.path.join(os.environ['PIPEBOX_DIR'],"libwcl/%s" % LIBNAME)
    newlib = newlib[newlib.find('/home'):]
    f = open(template,'r')
    fh = f.read()
    fh = replace_fh(fh,'{MYWCLDIR}',subst=newlib)
    fh = replace_fh(fh,'{USER}',   subst=args.user)
    fh = replace_fh(fh,'{DB_SECTION}',   subst=args.db_section)
    fh = replace_fh(fh,'{ARCHIVE_NAME}',   subst=args.archive_name)
    fh = replace_fh(fh,'{HTTP_SECTION}',   subst=args.http_section)
    fh = replace_fh(fh,'{REQNUM}',        subst=args.reqnum)
    fh = replace_fh(fh,'{TARGET_SITE}',   subst=args.target_site)
    fh = replace_fh(fh,'{LABELS}',        subst=args.labels)
    fh = replace_fh(fh,'{EUPS_PRODUCT}', subst=args.eups_product)
    fh = replace_fh(fh,'{EUPS_VERSION}', subst=args.eups_version)
    fh = replace_fh(fh,'{CAMPAIGN}',  subst=args.campaign)
    fh = replace_fh(fh,'{PROJECT}',  subst=args.project)
    fh = replace_fh(fh,'{SCHEMA}',  subst=args.schema)
    fh = replace_fh(fh,'{EXPNUM}',  subst=EXPNUM)
    fh = replace_fh(fh,'{NITE}',    subst=NITE)
    fh = replace_fh(fh,'{BAND}',    subst=BAND)
    fh = replace_fh(fh,'{FRINGE_CASE}', subst=FRINGE_CASE)

    # The calibration block
    info = cals.get_cals_info(nite=NITE,archive_name=args.archive_name,db_section=args.db_section,verb=args.verb)
    calib_section = cals.construct_wcl_block(info,NITE,verb=args.verb)
    fh = replace_fh(fh,'{CALIB_SECTION}', subst=calib_section)
    
    # Create Directory
    
    try:
        pipebox_work = os.environ['PIPEBOX_WORK']
    except:
        print "must declare $PIPEBOX_WORK"
        sys.exit(1)
    dirname = os.path.join(pipebox_work,'files_submit_r{REQNUM}'.format(REQNUM=args.reqnum))
    if not os.path.isdir(dirname):
        print "# Creating directory %s" % dirname
        os.mkdir(dirname)

    # Write out the new file
    wclname = os.path.join(dirname,'firstcut_{EXPNUM}_{BAND}_{REQNUM}.des'.format(EXPNUM=EXPNUM,BAND=BAND,REQNUM=args.reqnum))
    print "# Creating: %s" % wclname
    newfile = open(wclname,'w')
    newfile.write(fh)
    newfile.close()
    return wclname

if __name__ == "__main__":

    # Get the options
    args  = cmdline()

    wclnames = []
    # Case 1, multiple expnum in filelist
    if os.path.exists(args.expnum):
        print "# Will read file: %s" % args.expnum
        for line in open(args.expnum).readlines():
            if line[0] == "#":
                continue
            EXPNUM = line.split()[0]
            wclname = write_wcl(EXPNUM,args)
            wclnames.append(wclname)
            
            
    # Case 2: single expnum
    else:
        wclname = write_wcl(args.expnum,args)
        wclnames.append(wclname)

    # Now we write the submit bash file
    submit_name = os.path.join(pipebox_work,'submitme_{EXPNUM}_{REQNUM}.sh'.format(EXPNUM=args.expnum,REQNUM=args.reqnum))
    subm = open(submit_name,'w')
    subm.write("#!/usr/bin/env bash\n\n")
    for wclname in wclnames:
        subm.write("dessubmit %s\nsleep 30\n" % wclname)

    os.chmod(submit_name, 0755)
    print "# To submit files:\n"
    print "\t ./%s\n " % submit_name
    
