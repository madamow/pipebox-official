#!/usr/bin/env python

import os,sys
from despydb import desdbi
from se_calib import calib_info as cals
from calib.getexposures import get_exposures
from datetime import datetime
from PipeBox import replace_fh

def cmdline():

    import argparse
    parser = argparse.ArgumentParser(description="Creates the submit wcl for a NITE\nwrite_precal_wcl_nite.py 20150206 1124")
    # The positional arguments
    parser.add_argument("nite", action="store",default=None,
                        help="Nite string, e.g., 20150206")
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


def write_wcl(NITE,args):

    # Get input bias and flat exposures
    bias_expnums,dflat_expnums = get_exposures(args.db_section,NITE)
    # Get template
    template = os.path.join(os.environ['PIPEBOX_DIR'],"libwcl/%s/submitwcl/precal_template.des" % (args.campaign))
    
    # Open template file and replace file-handle
    MYWCLDIR = os.path.join(os.environ['PIPEBOX_DIR'],"libwcl/%s" % (args.campaign))
    if MYWCLDIR.find('/home') > 0:
        MYWCLDIR = MYWCLDIR[MYWCLDIR.index('/home'):]
        
    f = open(template,'r')
    fh = f.read()
    fh = replace_fh(fh,'{MYWCLDIR}',subst=MYWCLDIR)
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
    fh = replace_fh(fh,'{NITE}',    subst=NITE)
    fh = replace_fh(fh,'{BIAS_EXPNUM}', subst=bias_expnums)
    fh = replace_fh(fh,'{DFLAT_EXPNUM}', subst=dflat_expnums)

    # The calibration block
    info = cals.get_cals_info(nite=NITE,archive_name=args.archive_name,db_section=args.db_section,verb=args.verb)
    calib_section = cals.construct_wcl_block(info,NITE,verb=args.verb)
    fh = replace_fh(fh,'{CALIB_SECTION}', subst=calib_section)
    
    # Create Directory
    
    dirname = os.path.join(pipebox_work,'files_submit_r{REQNUM}'.format(REQNUM=args.reqnum))
    if not os.path.isdir(dirname):
        print "# Creating directory %s" % dirname
        os.mkdir(dirname)

    # Write out the new file
    wclname = os.path.join(dirname,'precal_{NITE}_{REQNUM}.des'.format(NITE=NITE,REQNUM=args.reqnum))
    print "# Creating: %s" % wclname
    newfile = open(wclname,'w')
    newfile.write(fh)
    newfile.close()
    return wclname

if __name__ == "__main__":

    # Get the options
    args  = cmdline()

    try:
        pipebox_work = os.environ['PIPEBOX_WORK']
    except:
        print "must declare $PIPEBOX_WORK"
        sys.exit(1)
    wclnames = []
            
    # Case 1: single NITE
    wclname = write_wcl(args.nite,args)
    wclnames.append(wclname)

    # Now we write the submit bash file
    submit_name = os.path.join(pipebox_work,'submitme_{NITE}_{REQNUM}.sh'.format(NITE=args.nite,REQNUM=args.reqnum))
    subm = open(submit_name,'w')
    subm.write("#!/usr/bin/env bash\n\n")
    for wclname in wclnames:
        subm.write("dessubmit %s\nsleep 30\n" % wclname)

    os.chmod(submit_name, 0755)
    print "# To submit files:\n"
    print "\t %s\n " % submit_name
