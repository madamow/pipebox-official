#!/usr/bin/env python

import os,sys
from pipebox import pipebox_utils, jira_utils

def cmdline():

    import argparse
    parser = argparse.ArgumentParser(description="Creates the submit for hostname")
    # The positional arguments
    parser.add_argument("--db_section", action="store", default='db-destest',
                        choices=['db-desoper','db-destest'],
                        help="DB Section to query")
    parser.add_argument("--archive_name", default=None,   
                        help="Archive name (i.e. prodbeta or desar2home)")
    parser.add_argument("--http_section", default=None,   
                        help="DES Services http-section  (i.e. file-http-prodbeta)")
    parser.add_argument("--target_site", action="store", default='fermigrid-sl6',
                        help="Compute Target Site")
    parser.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit")
    parser.add_argument("--labels", action="store", default='me-tests',
                        help="Coma-separated labels")
    parser.add_argument("--reqnum",help='JIRA ticket number')
    parser.add_argument('--jira_parent',help='JIRA parent ticket under which\
                         new ticket will be created.')
    parser.add_argument('--jira_description',help='Description of ticket\
                         found in JIRA')
    parser.add_argument('--jira_project',default='DESOPS',help='JIRA project where \
                         ticket will be created, e.g., DESOPS')
    parser.add_argument('--jira_summary',help='Title of JIRA ticket. To submit multiple \
                         exposures under same ticket you can specify jira_summary')
    parser.add_argument('--jira_user',default = jira_utils.get_jira_user(),help='JIRA username')
    parser.add_argument('--jira_section',default='jira-desdm',help='JIRA section \
                         in .desservices.ini file')
    parser.add_argument("--eups_product", action="store", default='firstcut',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--eups_version", action="store", default='Y3Ndev+1',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--campaign", action="store", default='Y3T',
                        help="Name of the campaign")
    parser.add_argument("--project", action="store", default='ACT',
                        help="Name of the project ie. ACT/FM/etc")
    parser.add_argument("--savefiles", action="store_true", help="Write dessubmit file")

    args = parser.parse_args()

    return args


if __name__ == "__main__":

    # Get the options
    args  = cmdline()

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
    output_name = "%s_hostname_rendered_template.des" % (args.reqnum)
    output_path = os.path.join(args.pipebox_work,output_name)
    args.rendered_template_path = []
    args.rendered_template_path.append(output_path)
       
    if args.savefiles: 
        pipebox_utils.write_template(submit_template_path,output_path,args)
        bash_template_path = os.path.join("scripts","submitme_template.sh")
        # Current schema of writing dessubmit bash script
        bash_script_name = "submitme_hostname_%s_%s.sh" % (args.reqnum,args.target_site)
        bash_script_path= os.path.join(args.pipebox_work,bash_script_name)
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
