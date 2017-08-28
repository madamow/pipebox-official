import os
import sys
import re
import time
import pandas
from subprocess import Popen,PIPE,STDOUT
from commands import getstatusoutput
from datetime import datetime,timedelta
from pipebox import env

def write_template(template,outfile,args):
    """ Takes template (relative to jinja2 template dir), output name of 
        rendered template, and args namespace and writes rendered template"""
    print "Rendering template: {ofile}".format(ofile = outfile)
    config_template = env.get_template(template)

    try: args.submittime = datetime.now()
    except: args['submittime'] = datetime.now()
    rendered_config_template = config_template.render(args=args)
    
    with open(outfile,'w') as rendered_template:
        rendered_template.write(rendered_config_template)

def submit_command(submitfile,wait=30,logfile=None):
    """ Takes des submit file and executes dessubmit. Default sleep after
        sleep is 30 seconds. If provided a logfile will write stdout,stderr
        to specified logfile"""

    commandline = ['dessubmit',submitfile]
    command = Popen(commandline,stdout = PIPE, stderr = STDOUT, shell = False)
    output,error = output,error = command.communicate()
    print "Submitting {sfile}".format(sfile = submitfile)

    if logfile:
        for line in output: logfile.write(line)

    print "Sleeping for {sleep} seconds...".format(sleep=wait)
    time.sleep(wait)

    try:
        runid = re.findall("[a-zA-z0-9_-]*_r\d+p\d+",output)[0]
        unitname,run = runid.rsplit('_',1)
        attempt = run.split('p')[1]
        return (unitname,attempt)
    except:
        print output
        print 'Error in submission!'
        pass

def less_than_queue(pipeline=None,user=None,reqnum=None,queue_size=1000):
    """ Returns True if desstat count is less than specified queue_size,
        false if not"""
    if not pipeline:
        print "Must specify pipeline!"
        sys.exit(1)
    # Grepping pipeline out of desstat
    desstat_cmd = Popen(('desstat'),stdout=PIPE)
    grep_cmd = Popen(('grep',pipeline),stdin=desstat_cmd.stdout,stdout=PIPE)
    desstat_cmd.stdout.close()
    # Grepping user out of desstat
    if user:
        grep_user_cmd = Popen(('grep',user),stdin=grep_cmd.stdout,stdout=PIPE)
        grep_cmd.stdout.close()
    else:
        grep_user_cmd = grep_cmd
    # Grepping for reqnum out of desstat
    grep_reqnum_cmd = Popen(('grep',reqnum),stdin=grep_user_cmd.stdout,stdout=PIPE)
    if not user: grep_cmd.stdout.close()
    grep_user_cmd.stdout.close()
    # Counting remaining runs
    count_cmd = Popen(('wc','-l'),stdin=grep_reqnum_cmd.stdout,stdout=PIPE)
    grep_reqnum_cmd.stdout.close()

    output,error = count_cmd.communicate()
    if int(output) < int(queue_size):
        return True
    else:
        return False
"""
def less_than_queue(pipeline=None,user=None,queue_size=1000):
    if not pipeline:
        print "Must specify pipeline!"
        sys.exit(1)
    desstat_cmd = Popen(('desstat'),stdout=PIPE)
    grep_cmd = Popen(('grep',pipeline),stdin=desstat_cmd.stdout,stdout=PIPE)
    desstat_cmd.stdout.close()
    if user:
        grep_user_cmd = Popen(('grep',user),stdin=grep_cmd.stdout,stdout=PIPE)
        grep_cmd.stdout.close()
    else:
        grep_user_cmd = grep_cmd
    count_cmd = Popen(('wc','-l'),stdin=grep_user_cmd.stdout,stdout=PIPE)
    if not user: grep_cmd.stdout.close()
    grep_user_cmd.stdout.close()
    output,error = count_cmd.communicate()
    if int(output) < int(queue_size):
        return True
    else:
        return False
"""
def read_file(file):
    """Read file as generator"""
    with open(file) as listfile:
        for line in listfile: 
            if line.strip(): 
                if '#' not in line.strip():
                    yield line.strip()

def print_cron_info(pipeline,site=None,pipebox_work=None,cron_path='.'):
    print "\n"
    print "# To submit files (from dessub/descmp1):\n"
    print "\t ssh dessub/descmp1"
    print "\t crontab -e"
    print "\t add the following to your crontab:"
    print"\t 0,30 * * * * %s >> %s/%s_autosubmit.log 2>&1 \n" % (cron_path,pipebox_work,pipeline)

    # Print warning of Fermigrid credentials
    if 'fermi' in site:
        print "# For FermiGrid please make sure your credentials are valid"
        print "\t setenv X509_USER_PROXY $HOME/.globus/osg/user.proxy"
        print "\t voms-proxy-info --all"

def print_submit_info(pipeline,site=None,eups_stack=None,submit_file=None):
    print "\n"
    print "# To submit files (from dessub/descmp1):\n"
    print "\t ssh dessub/descmp1"
    print "\t setup -v %s %s" % (eups_stack[0],eups_stack[1])
    print "\t %s\n" % submit_file 

    # Print warning of Fermigrid credentials
    if 'fermi' in site:
        print "# For FermiGrid please make sure your credentials are valid"
        print "\t setenv X509_USER_PROXY $HOME/.globus/osg/user.proxy"
        print "\t voms-proxy-info --all"

def stop_if_already_running(script_name):
    """ Exits program if program is already running """
    l = getstatusoutput("ps aux | grep -e '%s' | grep -v grep | grep -v vim | awk '{print $2}'| awk '{print $2}' | grep $USER" % script_name)
    if l[1]:
        print "Already running.  Aborting"
        print l[1]
        sys.exit(0)

def rename_file(args):
    """ Rename submitfile with attempt number."""
    add_string = 'p%02d' % int(args.attnum)
    update_submitfile = args.submitfile.replace(args.target_site, 
                                                add_string + '_' + args.target_site)
    os.rename(args.submitfile,update_submitfile)
    return args.submitfile

def create_nitelist(min,max):
    min_date = datetime(int(min[:4]),int(min[4:6]),int(min[6:]))
    max_date = datetime(int(max[:4]),int(max[4:6]),int(max[6:]))
    daterange = pandas.date_range(min_date,max_date)
    daterange = [str(d.date()).replace('-','') for d in daterange]
    return daterange
    
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

def ask_string(question, default, check=None, passwd=False):

    import getpass

    ask_again = True
    answer = None
    while(ask_again):
        ask_again = False
        sys.stdout.write("\n" + question + "\n")
        sys.stdout.write("[%s] : " % default)
        flush(sys.stdout)
        if passwd:
            answer = getpass.getpass('')
            return answer
        
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


def cycle_list_index(index,options):
    k = index % len(options)
    return options[k]
