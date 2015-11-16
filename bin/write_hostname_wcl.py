#!/usr/bin/env python

import os,sys
from pipebox import pipebox_utils, jira_utils, commandline


# Get the options
pipeargs = commandline.PipeArgs()
args  = pipeargs.cmdline()[1]

try:
    pipebox_work = os.environ['PIPEBOX_WORK']
except:
    print "must declare $PIPEBOX_WORK"
    sys.exit(1)

args.pipebox_work = pipebox_work

# Create JIRA ticket
new_reqnum,new_jira_parent = jira_utils.create_ticket(args.jira_section,args.jira_user,
                                          description=args.jira_description,
                                          summary=args.jira_summary,
                                          ticket=args.reqnum,parent=args.jira_parent,
                                          use_existing=True)    

args.reqnum,args.jira_parent = new_reqnum,new_jira_parent

submit_template_path = os.path.join("pipelines/hostname","hostname_template.des")
req_dir = 'r%s' % args.reqnum
output_dir = os.path.join(args.pipebox_work,req_dir)
if not os.path.isdir(output_dir):
    os.makedirs(output_dir)
output_name = "%s_hostname_rendered_template.des" % (args.reqnum)
output_path = os.path.join(output_dir,output_name)
args.rendered_template_path = []
args.rendered_template_path.append(output_path)
   
if args.savefiles: 
    pipebox_utils.write_template(submit_template_path,output_path,args)
    bash_template_path = os.path.join("scripts","submitme_template.sh")
    # Current schema of writing dessubmit bash script
    bash_script_name = "submitme_hostname_%s_%s.sh" % (args.reqnum,args.target_site)
    bash_script_path= os.path.join(output_dir,bash_script_name)
    # Write bash script
    pipebox_utils.write_template(bash_template_path,bash_script_path,args)

    os.chmod(bash_script_path, 0755)

    pipebox_utils.print_submit_info('hostname',site=args.target_site,
                                          eups_product=args.eups_product,
                                          eups_version=args.eups_version,
                                          submit_file=bash_script_path)

else:
    # Placeholder for submitting jobs
    pipebox_utils.submit_command(output_path)
