#!/usr/bin/env python

import os,sys
from datetime import datetime
from argparse import ArgumentParser
import pandas as pd
from pipebox import pipebox_utils,jira_utils,query,commandline
from opstoolkit import common

widefield = commandline.WidefieldArgs()  
args = widefield.cmdline()

if args.paramfile:
    args = pipebox_utils.update_from_param_file(args)
    args = pipebox_utils.replace_none_str(args)

try:
    args.pipebox_work = os.environ['PIPEBOX_WORK']
    args.pipebox_dir  = os.environ['PIPEBOX_DIR']
except:
    print "must declare $PIPEBOX_WORK"
    sys.exit(1)    

# For each use-case create exposures list and exposure dataframe
cur = query.WideField(args.db_section)

if args.exptag:
    args.exposure_list = cur.get_expnums_from_tag(args.exptag)
    args.exposure_df   = pd.DataFrame(args.exposure_list,columns=['expnum'])
elif args.expnum: 
    args.exposure_list = args.expnum.split(',')
    args.exposure_df   = pd.DataFrame(args.exposure_list,columns=['expnum'])
elif args.list: 
    args.exposure_list = list(pipebox_utils.read_file(args.list))
    args.exposure_df   = pd.DataFrame(args.exposure_list,columns=['expnum'])
elif args.csv: 
    args.exposure_df = pd.read_csv(args.csv,sep=args.delimiter)
    args.exposure_df.columns = [col.lower() for col in args.exposure_df.columns]
    args.exposure_list = list(args.exposure_df['expnum'].values)
    
# Update dataframe for each exposure and add band,nite if not exists
args.exposure_df = cur.update_df(args.exposure_df) 

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
        args.exposure_df.loc[index,'reqnum'] = new_reqnum
    try:
        args.exposure_df.loc[index,('jira_parent')] = new_jira_parent
    except: 
        args.exposure_df.insert(len(args.exposure_df.columns),'jira_parent',None)
        args.exposure_df.loc[index,'jira_parent'] = new_jira_parent

# Define archive_name if undefined
if not args.archive_name:
    if args.db_section  == 'db-destest': args.archive_name = 'prodbeta'
    if args.db_section  == 'db-desoper': args.archive_name = 'desar2home'

# Render and write templates
campaign_path = "pipelines/finalcut/%s/submitwcl" % args.campaignlib
if args.template_name:
    submit_template_path = os.path.join(campaign_path,args.template_name)
else:
    submit_template_path = os.path.join(campaign_path,"finalcut_submit_template.des")
bash_template_path = os.path.join("scripts","submitme_template.sh")
args.rendered_template_path = []
# Create templates for each entry in dataframe
reqnum_count = len(args.exposure_df.groupby(by=['reqnum']))
for index,row in args.exposure_df.iterrows():
    args.expnum,args.band,args.nite,args.reqnum, args.jira_parent= row['expnum'],row['band'],row['nite'],int(row['reqnum']),row['jira_parent']
    if args.epoch:
        args.epoch_name = args.epoch
    else:
        args.epoch_name = cur.find_epoch(row['expnum'])
    # Making output directories and filenames
    req_dir = 'r%s' % args.reqnum
    output_dir = os.path.join(args.pipebox_work,req_dir)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    output_name = "%s_%s_r%s_%s_finalcut_rendered_template.des" % (args.expnum,args.band,args.reqnum,args.target_site)
    output_path = os.path.join(output_dir,output_name)

    # If ngix -- cycle trough server's list
    if args.nginx:
        args.nginx_server = pipebox_utils.cycle_list_index(index,['descmp0','descmp4','desftp', 'deslogin'])

    # Writing template
    pipebox_utils.write_template(submit_template_path,output_path,args)

    if args.savefiles:
        # Current schema of writing dessubmit bash script 
        if reqnum_count > 1:
            bash_script_name = "submitme_%s_%s.sh" % (datetime.now().strftime('%Y-%m-%dT%H:%M'),args.target_site)
        else:
            bash_script_name = "submitme_%s_%s.sh" % (args.reqnum,args.target_site)
        bash_script_path= os.path.join(output_dir,bash_script_name)
        args.rendered_template_path.append(bash_script_path)
    else:
        # If less than queue size submit exposure
        if pipebox_utils.less_than_queue('finalcut',args.queue_size):
            pipebox_utils.submit_command(output_path)

if args.savefiles:
    # Writing bash submit scripts
    pipebox_utils.write_template(bash_template_path,bash_script_path,args)
    os.chmod(bash_script_path, 0755)
    pipebox_utils.print_submit_info('finalcut',site=args.target_site,
                                               eups_product=args.eups_product,
                                               eups_version=args.eups_version,
                                               submit_file=bash_script_path)
