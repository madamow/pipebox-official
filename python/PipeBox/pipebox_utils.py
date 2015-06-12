import os
import sys
import time
from despydb import desdbi

from se_calib import calib_info as cals
from datetime import datetime

def replace_file(file_path, pattern, subst='', prompt=''):
    """ Replace in place for file"""
    if prompt != '':
        subst = raw_input(prompt+' ')
    fh, abs_path = mkstemp()
    with open(abs_path,'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    close(fh)
    remove(file_path)
    move(abs_path, file_path)


def replace_fh(fh,pattern,subst='',prompt=''):
    """ Replace in place for file-handle"""
    if prompt != '':
        subst = raw_input(prompt+' ')
    fh = fh.replace(pattern,subst)
    return fh


def get_expnum_info(expnum,db_section='db-destest'):

    dbh = desdbi.DesDbi(section=db_section)
    cur = dbh.cursor()
    QUERY = '''SELECT nite,band from exposure where expnum={expnum}'''
    cur.execute(QUERY.format(expnum=expnum))
    nite, band = cur.fetchone()
    return nite, band


def write_wcl(EXPNUM,args):

    # Get NITE and BAND for expnum
    NITE, BAND   = get_expnum_info(EXPNUM,args.db_section)
    template_path = os.path.join(os.environ['PIPEBOX_DIR'],"libwcl/%s/submitwcl/%s_template.des" % (args.libname,args.template))
    pipebox_work = os.environ['PIPEBOX_WORK']

    # Fring Case
    if BAND in ['z','Y']:
        FRINGE_CASE = 'fringe'
    else:
        FRINGE_CASE = 'nofringe'

    # Open template file and replace file-handle
    MYWCLDIR = os.path.join(os.environ['PIPEBOX_DIR'],"libwcl/%s" % args.libname)
    if MYWCLDIR.find('/home') > 0:
        MYWCLDIR = MYWCLDIR[MYWCLDIR.index('/home'):]
        
    f = open(template_path,'r')
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
    fh = replace_fh(fh,'{EXPNUM}',  subst=EXPNUM)
    fh = replace_fh(fh,'{NITE}',    subst=NITE)
    fh = replace_fh(fh,'{BAND}',    subst=BAND)
    fh = replace_fh(fh,'{FRINGE_CASE}', subst=FRINGE_CASE)
    fh = replace_fh(fh,'{CCD_LIST}', subst=args.ccd_list)
    # For rerun's
    fh = replace_fh(fh,'{REQNUM_INPUT}', subst=args.reqnum_input)
    fh = replace_fh(fh,'{ATTNUM_INPUT}', subst=args.attnum_input)

   # The calibration block
    if fh.find('{CALIB_SECTION}')>=0:
        info = cals.get_cals_info(nite=NITE,archive_name=args.archive_name,db_section=args.db_section,verb=args.verb)
        calib_section = cals.construct_wcl_block(info,NITE,verb=args.verb)
        fh = replace_fh(fh,'{CALIB_SECTION}', subst=calib_section)
    
    # Create Directory
    dirname = os.path.join(pipebox_work,'files_submit_r{REQNUM}'.format(REQNUM=args.reqnum))
    if not os.path.isdir(dirname):
        print "# Creating directory %s" % dirname
        os.mkdir(dirname)

    # Write out the new file
    wclname = os.path.join(dirname,'{TEMPLATE}_{EXPNUM}_{BAND}_{REQNUM}.des'.format(TEMPLATE=args.template,EXPNUM=EXPNUM,BAND=BAND,REQNUM=args.reqnum))
    print "# Creating: %s" % wclname
    newfile = open(wclname,'w')
    newfile.write(fh)
    newfile.close()
    return wclname



# --------------------------------------------------------------
# These functions were copied/adapted from desdm_eupsinstal.py

def flush(f):
    f.flush();
    time.sleep(0.05)

def check_file(filename):
    if os.path.exists(filename):
        return
    else:
        return " File not found "

def ask_string(question, default, check=None):
   
    ask_again = True
    answer = None
    while(ask_again):
        ask_again = False
        sys.stdout.write("\n" + question + "\n")
        sys.stdout.write("[%s] : " % default)
        flush(sys.stdout)
        line = sys.stdin.readline()
        if line:
            line = line.strip()
            answer = None
            if not line:
                answer = default
            else:
                answer = line
            
            if check != None:
                message = check(answer)
                if message:
                    sys.stdout.write("\n")
                    flush(sys.stdout)
                    sys.stderr.write(message + "\n")
                    flush(sys.stderr)
                    ask_again = True
        else:
            sys.stdout.write("\n")
            flush(sys.stdout)
            sys.stderr.write("Reached end of input. Aborting.\n")
            sys.exit(2)
    return answer

def ask_bool(question, default):
    ask_again = True
    while ask_again:
        ask_again = False
        sys.stdout.write("\n" + question + "\n")
        if default:
            sys.stdout.write("[yes] : ")
        else:
            sys.stdout.write("[no] : ")
        flush(sys.stdout)
        line = sys.stdin.readline()
        if line:
            line = line.strip().lower()
            if not line:
                answer = default
            else:
                if line == "y" or line == "yes":
                    answer = True
                elif line == "n" or line == "no":
                    answer = False
                else:
                    answer = None
                    ask_again = True
                    sys.stdout.write("\n")
                    flush(sys.stdout)
                    sys.stderr.write("Please answer with 'yes' or 'no'.\n")
                    flush(sys.stderr)
            if answer:
                return answer

# --------------------------------------------------------------
