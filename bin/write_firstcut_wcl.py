#!/usr/bin/env python

import os,sys
from datetime import datetime
import time
from pipebox import pipebox_utils,pipeline

widefield = pipeline.WideField()
args = widefield.args

#add this to default arguments in pipeargs
args.pipebox_dir,args.pipebox_work=widefield.pipebox_dir,widefield.pipebox_work

if args.paramfile:
    args = pipebox_utils.update_from_param_file(args)
    args = pipebox_utils.replace_none_str(args)

if args.auto:
    # Set crontab path
    cron_template_path = os.path.join('scripts',"cron_firstcut_autosubmit_template.sh")
    cron_submit_path = os.path.join(widefield.pipebox_work,"cron_firstcut_autosubmit_rendered_template.sh")
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
        widefield.auto()

else: 
    # For each use-case create exposures list and exposure dataframe
    widefield.ticket(args = args)
    # Render and write templates
    campaign_path = "pipelines/firstcut/%s/submitwcl" % args.campaign
    if args.template_name:
        submit_template_path = os.path.join(campaign_path,args.template_name)
    else:
        submit_template_path = os.path.join(campaign_path,"firstcut_submit_template.des")
    bash_template_path = os.path.join("scripts","submitme_template.sh")
    args.rendered_template_path = []
    # Create templates for each entry in dataframe
    reqnum_count = len(args.dataframe.groupby(by=['reqnum']))
    for index,row in args.dataframe.iterrows():
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
            else:
                while not pipebox_utils.less_than_queue('firstcut',queue_size=args.queue_size):
                    time.sleep(30)
                else:
                    pipebox_utils.submit_command(output_path)

    if args.savefiles:
        # Writing bash submit scripts
        pipebox_utils.write_template(bash_template_path,bash_script_path,args)
        os.chmod(bash_script_path, 0755)
        pipebox_utils.print_submit_info('firstcut',site=args.target_site,
                                                   eups_product=args.eups_product,
                                                   eups_version=args.eups_version,
                                                   submit_file=bash_script_path)
