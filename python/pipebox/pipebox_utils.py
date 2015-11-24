import os
import sys
import time
from subprocess import Popen,PIPE,STDOUT
from commands import getstatusoutput
from despydb import desdbi
from datetime import datetime,timedelta
from pipebox import env

ALL_CCDS='1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,62' # No 2,31,61

def write_template(template,outfile,args):
    """ Takes template (relative to jinja2 template dir), output name of 
        rendered template, and args namespace and writes rendered template"""
    config_template = env.get_template(template)
    rendered_config_template = config_template.render(args=args)
    
    args.submittime = datetime.now()
    with open(outfile,'w') as rendered_template:
        rendered_template.write(rendered_config_template)

def submit_command(submitfile,wait=30,logfile=None):
    """ Takes des submit file and executes dessubmit. Default sleep after
        sleep is 30 seconds. If provided a logfile will write stdout,stderr
        to specified logfile"""
    commandline = ['dessubmit',submitfile]
    if logfile:
        command = Popen(commandline,stdout = logfile, stderr = logfile, shell = False)
    else:   
        command = Popen(commandline,stdout = PIPE, stderr = STDOUT, shell = False)
    time.sleep(wait)

def less_than_queue(pipeline,queue_size=1000):
    """ Returns True if desstat count is less than specified queue_size, 
        false if not"""
    desstat_cmd = Popen(('desstat'),stdout=PIPE)
    grep_cmd = Popen(('grep',pipeline),stdin=desstat_cmd.stdout,stdout=PIPE)
    desstat_cmd.stdout.close()
    count_cmd = Popen(('wc','-l'),stdin=grep_cmd.stdout,stdout=PIPE)
    grep_cmd.stdout.close()
    output,error = count_cmd.communicate()
    if int(output) < int(queue_size):
        return True
    else:
        return False

def read_file(file):
    """Read file as generator"""
    with open(file) as listfile:
        for line in listfile: yield line.strip()

def update_from_param_file(args,delimiter='='):
    file = args.paramfile
    with open(file,'r') as paramfile:
        lines = paramfile.readlines()
    param_dict = {row.split(delimiter)[0].strip():row.split(delimiter)[1].strip()
                  for row in lines} 

    args_dict = vars(args)    
    for key,val in args_dict.items():
        for pkey,pval in param_dict.items():
            if pkey==key:
                args_dict[key] = param_dict[key]
    return args

def replace_none_str(args):
    args_dict = vars(args)
    for key,val in args_dict.items():
        if val=='None':
            args_dict[key] = None
        elif not val:
            args_dict[key] = None
    return args

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

def print_submit_info(pipeline,site=None,eups_product=None,eups_version=None,submit_file=None):
    print "\n"
    print "# To submit files (from dessub/descmp1):\n"
    print "\t ssh dessub/descmp1"
    print "\t setup -v %s %s" % (eups_product,eups_version)
    print "\t %s\n" % submit_file 

    # Print warning of Fermigrid credentials
    if 'fermi' in site:
        print "# For FermiGrid please make sure your credentials are valid"
        print "\t setenv X509_USER_PROXY $HOME/.globus/osg/user.proxy"
        print "\t voms-proxy-info --all"

def stop_if_already_running(script_name):
    """ Exits program if program is already running """
    l = getstatusoutput("ps aux | grep -e '%s' | grep -v grep | grep -v vim | awk '{print $2}'| awk '{print $2}' " % script_name)
    if l[1]:
        print "Already running.  Aborting"
        print l[1]
        sys.exit(0)
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
