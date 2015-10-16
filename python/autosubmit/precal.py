#! /usr/bin/env python

import argparse
import os
import sys
from subprocess import Popen,PIPE
import time
from datetime import datetime
import shlex

from opstoolkit import jiracmd
from Pipebox import query

def make_comment(date,nite,reqnum):
    comment = """
              Autosubmitted at %s
              -----
              h3. Nite: %s
              -----
              * Reqnum: %s
              * Attnum: 01

              h5. Run Status:

              h6. Tagged:
              h6. Datastate:

              h5. Location:  /archive_data/desarchive/OPS/precal/%s-r%s/p01
              """ % (date,nite,reqnum,nite,reqnum)
    return comment

def run(args):    
    # Check to see if precal has been submitted with dflat_nite.
    inputs = query.Precal(args.db_section)
    if args.nite:
        nite = args.nite
    else:
        nite = inputs.get_max()[1]
    if inputs.check_submitted(nite) != 0:
        submitted_string = "%s: %s previously submitted! Exiting...(l32)" % (datetime.now(),nite)
        print submitted_string
        sys.exit()
    else:
        # Create log file
        log = '%s_precal_submit_%s.log' % (nite,time.strftime("%X"))
        logfile = open(log,'a')

        # Run check_for_precal_inputs.py
        print "Running check_for_precal_inputs.py"
        try: opstoolkit_dir = os.environ['OPSTOOLKIT_DIR']
        except: 
            print "Must setup opstoolkit!"
            sys.exit(1)
        check_precal_string = "check_for_precal_inputs.py --section %s --band u,g,r,i,z,Y,VR --CalReq 20,10,10,10,10,10,10,10 --night %s " % (args.db_section,nite)
        print check_precal_string
        check_precal_command_pieces = shlex.split(check_precal_string)
        check_precal_command = Popen(check_precal_command_pieces,stdout = logfile, stderr = logfile, shell = False)
        check_precal_command.communicate()
        runornot = 'no'
        for line in open(log).readlines():
            if "LETS ROCK!" in line:
                runornot = 'yes'
        if runornot == 'no':
            calib_string = "%s Not enough calibrations yet. Exiting..." % datetime.now()
            print calib_string
            sys.exit(0)
        if runornot == 'yes':
            # Check to see if Jira ticket exists, if not make one.
            Jira = jiracmd.Jira(args.jira_section)
            exists = Jira.search_for_issue(args.jira_parent,nite)
            if exists[1] == 0:
                # Create JIRA ticket
                description = 'Input nite: %s' % nite
                subticket = str(Jira.create_jira_subtask(args.jira_parent,nite,description,args.jira_user))
                reqnum = subticket.split('-')[1]
                key = 'DESOPS-%s' % reqnum
            else:
                reqnum = str(exists[0][0].key).split('-')[1]
                key = 'DESOPS-%s' % reqnum
            # Creating submit file
            submit = 'precal_%s_submit.des' % (nite)
            if os.path.isfile(submit):
                submitted_string = "%s: %s_r%s previously submitted! Exiting...(l72)" %(datetime.now(),nite,reqnum)
                print submitted_string
                sys.exit(0)
            else:
                enough_calib_string = "%s Found calibrations. Beginning processing..." % datetime.now()
                print enough_calib_string
                # Add template into newly created submit file.
                submitfile = open(submit,'w')
                template = open('precal_submit.des','r').read()
                submitfile.write(template)
                submitfile.close()

                # Add nite,reqnum,label to submitfile.
                includes = []
                add_nite = "%s = %s" % ('nite',nite)
                includes.append(add_nite)
                reqnumstr = "%s = %s" % ('reqnum',reqnum)
                includes.append(reqnumstr)
                label = 'label = precal_%s' % nite
                includes.append(label)
               
                # Create input expnum lists
                if list_dir:
                    lists_dir = args.listdir
                else:
                    lists_dir = os.getcwd()
                listdir = "%s/%s" % (lists_dir,nite)
                if not os.path.isdir(listdir):
                    os.makedirs(listdir)
                bias_file = "%s/%s_bias_exposures.list" % (listdir,nite)
                flat_file = "%s/%s_dflat_exposures.list" % (listdir,nite)
                bias_exp_file = open(bias_file,'w')
                flat_exp_file = open(flat_file,'w')
                for lines in open(log).readlines():
                    if 'bias_expnum' in lines:
                        bias_exp_file.write(lines)
                    if 'dflat_expnum' in lines:
                        flat_exp_file.write(lines)
                bias_exp_file.close()
                flat_exp_file.close()
   
                # Replace include lines in submit file.
                fr = open(submit).read()
                fr = fr.replace('$$$$',nite)
                fw = open(submit,'w')
                fw.write(fr)
                fw.close()
                fa = open(submit,'a')
                for item in includes:
                    line = "%s\n" % item
                    fa.write(line) 
                fa.close()
            
                # Executing dessubmit command
                submit_string = "dessubmit %s" % submit
                submit_command = shlex.split(submit_string)
                command = Popen(submit_command,stdout = logfile, stderr = logfile, shell = False)
                comment = make_comment(datetime.now(),nite,reqnum)
                Jira.add_jira_comment(key,comment)
