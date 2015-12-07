#!/usr/bin/env python

import os,sys
from datetime import datetime
import time
from pipebox import pipebox_utils,pipeline

widefield = pipeline.WideField()
args,cur = widefield.args,widefield.cur

#add this to default arguments in pipeargs
args.pipebox_dir,args.pipebox_work=widefield.pipebox_dir,widefield.pipebox_work

if args.paramfile:
    args = pipebox_utils.update_from_param_file(args)
    args = pipebox_utils.replace_none_str(args)

# create JIRA ticket per nite and add jira_id,reqnum to dataframe
widefield.ticket(args = args)

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
reqnum_count = len(args.dataframe.groupby(by=['reqnum']))
for index,row in args.dataframe.iterrows():
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
        #args.nginx_server = pipebox_utils.cycle_list_index(index,['descmp0','descmp4','desftp', 'deslogin'])
        args.nginx_server = pipebox_utils.cycle_list_index(index,['desnginx', 'dessub'])

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
        if pipebox_utils.less_than_queue('finalcut',queue_size=args.queue_size):
            pipebox_utils.submit_command(output_path)
        else:
            while not pipebox_utils.less_than_queue('finalcut',queue_size=args.queue_size):
                time.sleep(30)
            else:
                pipebox_utils.submit_command(output_path)
        

if args.savefiles:
    # Writing bash submit scripts
    pipebox_utils.write_template(bash_template_path,bash_script_path,args)
    os.chmod(bash_script_path, 0755)
    pipebox_utils.print_submit_info('finalcut',site=args.target_site,
                                               eups_product=args.eups_product,
                                               eups_version=args.eups_version,
                                               submit_file=bash_script_path)
