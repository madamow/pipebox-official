#!/usr/bin/env python

import os,sys
from datetime import datetime
import time
from argparse import ArgumentParser
import pandas as pd
from pipebox import pipebox_utils,jira_utils,query,commandline
from autosubmit import firstcut

widefield = commandline.WidefieldArgs()
args = widefield.cmdline()

if args.paramfile:
    args = pipebox_utils.update_from_param_file(args)
    args = pipebox_utils.replace_none_str(args)

try:
    args.pipebox_work = os.environ['PIPEBOX_WORK']
    args.pipebox_dir = os.environ['PIPEBOX_DIR']
except:
    print "must declare $PIPEBOX_WORK"
    sys.exit(1)    

if args.auto:
    # Set crontab path
    cron_template_path = os.path.join('scripts',"cron_firstcut_autosubmit_template.sh")
    cron_submit_path = os.path.join(args.pipebox_work,"cron_firstcut_autosubmit_rendered_template.sh")
    if args.savefiles:
        # Writing template
        pipebox_utils.write_template(cron_template_path,cron_submit_path,args)
        os.chmod(cron_submit_path, 0755)
        pipebox_utils.print_cron_info('firstcut',site=args.target_site,
                                                   pipebox_work=args.pipebox_work,
                                                   cron_path=cron_submit_path)
    else:
        # Run autosubmit code directly
        # Will run once, but if put in crontab will run however you specify in cron
    
        # Kill current process if cron is running from last execution
        pipebox_utils.stop_if_already_running(os.path.basename(__file__))
        time.sleep(5)
        firstcut.run(args)

else: 
    cur = query.FirstCut(args.db_section)

    # For each use-case create exposures list and exposure dataframe
    if args.exptag:
        args.exposure_list = query.get_expnums_from_tag(args.exptag)
        args.exposure_df = pd.DataFrame(args.exposure_list,columns=['expnum'])
    elif args.expnum: 
        args.exposure_list = args.expnum.split(',')
        args.exposure_df = pd.DataFrame(args.exposure_list,columns=['expnum'])
    elif args.list: 
        args.exposure_list = list(pipebox_utils.read_file(args.list))
        args.exposure_df = pd.DataFrame(args.exposure_list,columns=['expnum'])
    elif args.csv: 
        args.exposure_df = pd.read_csv(args.csv,sep=args.delimiter)
        args.exposure_df.columns = [col.lower() for col in args.exposure_df.columns]
        args.exposure_list = list(args.exposure_df['expnum'].values)
    
    # Update dataframe for each exposure and add band,nite if not exists
    cur.update_df(args.exposure_df) 
    
    args.exposure_df =args.exposure_df.fillna(False) 
    nite_group = args.exposure_df.groupby(by=['nite'])
    for nite,group in nite_group:
        # create JIRA ticket per nite and add jira_id,reqnum to dataframe
        index = args.exposure_df[args.exposure_df['nite'] == nite].index
        
        if args.jira_summary:
            jira_summary = args.jira_summary
        else: 
            jira_summary = str(nite)
        if args.reqnum:
            reqnum = args.reqnum
        else:
            reqnum = None
        if args.jira_parent:
            jira_parent = args.jira_parent
        else:
            jira_parent = None
        # Create JIRA ticket
        new_reqnum,new_jira_parent = jira_utils.create_ticket(args.jira_section,args.jira_user,
                                              description=args.jira_description,
                                              summary=jira_summary,
                                              ticket=reqnum,parent=jira_parent,
                                              use_existing=True)
        # Update dataframe with reqnum, jira_id
        # If row exists replace value, if not insert new column/value
        try:
            args.exposure_df.loc[index,('reqnum')] = new_reqnum
        except: 
            args.exposure_df.insert(len(args.exposure_df.columns),'reqnum',None)
            args.exposure_df.loc[index,('reqnum')] = new_reqnum
        try:
            args.exposure_df.loc[index,('jira_parent')] = new_jira_parent
        except: 
            args.exposure_df.insert(len(args.exposure_df.columns),'jira_parent',None)
            args.exposure_df.loc[index,('jira_parent')] = new_jira_parent
    
    # Render and write templates
    campaign_path = "pipelines/firstcut/%s/submitwcl" % args.campaign
    if args.template_name:
        submit_template_path = os.path.join(campaign_path,args.template_name)
    else:
        submit_template_path = os.path.join(campaign_path,"firstcut_submit_template.des")
    bash_template_path = os.path.join("scripts","submitme_template.sh")
    args.rendered_template_path = []
    # Create templates for each entry in dataframe
    reqnum_count = len(args.exposure_df.groupby(by=['reqnum']))
    for index,row in args.exposure_df.iterrows():
        args.expnum,args.band,args.nite,args.reqnum,args.jira_parent = row['expnum'],row['band'],row['nite'],int(row['reqnum']),row['jira_parent']
        req_dir = 'r%s' % args.reqnum
        output_dir = os.path.join(args.pipebox_work,req_dir)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
        output_name = "%s_%s_r%s_firstcut_rendered_template.des" % (args.expnum,args.band,args.reqnum)
        output_path = os.path.join(output_dir,output_name)
        # Writing template
        pipebox_utils.write_template(submit_template_path,output_path,args)

        if args.savefiles:
            # Current schema of writing dessubmit bash script 
            if reqnum_count > 1:
                bash_script_name = "submitme_%s_%s.sh" % (datetime.now().strftime('%Y-%m-%dT%H:%M'),args.target_site)
            else:
                bash_script_name = "submitme_%s_%s.sh" % (args.reqnum,args.target_site)
            bash_script_path= os.path.join(output_dir,bash_script_name)
            args.rendered_template_path.append(output_path)

        else:
            # If less than queue size submit exposure
            if pipebox_utils.less_than_queue('firstcut',queue_size=args.queue_size):
                pipebox_utils.submit_command(output_path)

    if args.savefiles:
        # Writing bash submit scripts
        pipebox_utils.write_template(bash_template_path,bash_script_path,args)
        os.chmod(bash_script_path, 0755)
        pipebox_utils.print_submit_info('firstcut',site=args.target_site,
                                                   eups_product=args.eups_product,
                                                   eups_version=args.eups_version,
                                                   submit_file=bash_script_path)
