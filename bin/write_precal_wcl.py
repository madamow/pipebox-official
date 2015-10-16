#!/usr/bin/env python

import os,sys
from argparse import ArgumentParser
import pandas as pd
from PipeBox import pipebox_utils,jira_utils,query
from autosubmit import precal

def cmdline():
    # Create command line arguments
    parser = ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(help='sub-command help')
    parser_m = subparsers.add_parser('manual',help='Manual options')
    parser_m.set_defaults(mode='manual')
    parser_m.add_argument('--db_section',required=True,help = "e.g., db-desoper or db-destest")
    parser_m.add_argument('--csv',help='')
    parser_m.add_argument('--ccdnum',default=pipebox_utils.ALL_CCDS,help='')
    parser_m.add_argument('--bias_list',help='')
    parser_m.add_argument('--dflat_list',help='')
    parser_m.add_argument('--target_site',help='')
    parser_m.add_argument('--schema',help='')
    parser_m.add_argument('--http_section',help='')
    parser_m.add_argument('--archive_name',help='')
    parser_m.add_argument('--jira_parent',help='')
    parser_m.add_argument('--jira_description',help='')
    parser_m.add_argument('--jira_project',default='DESOPS',help='')
    parser_m.add_argument('--jira_summary',help='')
    parser_m.add_argument('--jira_user',help='')
    parser_m.add_argument('--jira_section',help='')
    parser_m.add_argument('--reqnum',help='')
    parser_m.add_argument('--delimiter',default=',',help='For csv')
    parser_m.add_argument('--campaign',help='')
    parser_m.add_argument('--project',default='ACT',help='')
    parser_m.add_argument('--eups_version',help='')
    parser_m.add_argument('--eups_product',help='')
    parser_m.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit") 
    parser_m.add_argument('--savefiles',action='store_true',help='Saves submit files to submit later.')

    parser_a = subparsers.add_parser('auto',help='Auto options')
    parser_a.set_defaults(mode='auto')
    parser_a.add_argument('--db_section',required=True,help = "e.g., db-desoper or db-destest")
    parser_a.add_argument('--nite',help='')
    parser_a.add_argument('--target_site',help='')
    parser_a.add_argument('--http_section',help='')
    parser_a.add_argument('--archive_name',help='')
    parser_a.add_argument('--schema',help='')
    parser_a.add_argument('--jira_parent',help='')
    parser_a.add_argument('--jira_description',help='')
    parser_a.add_argument('--jira_project',default='DESOPS',help='')
    parser_a.add_argument('--jira_summary',help='')
    parser_a.add_argument('--jira_user',help='')
    parser_a.add_argument('--jira_section',help='')
    parser_a.add_argument('--eups_product',help='')
    parser_a.add_argument('--eups_version',help='')
    parser_a.add_argument('--campaign',help='')
    parser_a.add_argument('--project',default='ACT',help='')
    parser_a.add_argument('--queue_size',help='')
    parser_a.add_argument('--bias_list',help='')
    parser_a.add_argument('--dflat_list',help='')
    parser_a.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit")
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = cmdline()

    try:
        args.pipebox_work = os.environ['PIPEBOX_WORK']
        args.pipebox_dir = os.environ['PIPEBOX_DIR']
    except:
        print "must declare $PIPEBOX_WORK"
        sys.exit(1)    

    if args.mode=='auto':
        # Set crontab
        cron_template_path = os.path.join(args.pipebox_dir,"cron_precal_autosubmit_template.sh")
        cron_submit_path = os.path.join(args.pipebox_work,"cron_precal_autosubmit_rendered_template.sh")
        if args.savefiles:
            # Writing template
            pipebox_utils.write_template(cron_template_path,cron_submit_path,args)
            # write cron template with instructions
        else:
            # Run autosubmit code directly and modify cron using python-crontab
            #precal.run(args)
            pass
    
    if args.mode=='manual': 
        if args.nite: 
            args.nite_list = args.expnum.split(',')
            args.nite_df = pd.DataFrame(args.nite_list,columns=['expnum'])
        elif args.list: 
            with open(args.list) as listfile:
                args.nite_list = listfile.read().splitlines()
            args.nite_df = pd.DataFrame(args.nite_list,columns=['expnum'])
        elif args.csv: 
            args.nite_df = pd.read_csv(args.csv,sep=args.delimiter)
            args.nite_df.columns = [col.lower() for col in args.nite_df.columns]
            args.nite_list = list(args.nite_df['expnum'].values)
        
        # Update dataframe for each nite and add band,nite if not exists
        cur = query.Precal(args.db_section)
        nite_info = cur.update_df(args.nite_df) 
            
        nite_group = args.nite_df.groupby(by=['nite'])
        for nite,group in nite_group:
            # create JIRA ticket per nite and add jira_id,reqnum to dataframe
            index = args.nite_df[args.nite_df['nite'] == nite].index
            if args.jira_summary: summary = args.jira_summary
            else: summary = str(nite)
            if args.reqnum:
                reqnum = args.reqnum
            else:
                try:
                    reqnum = str(int(args.nite_df.loc[index,('reqnum')].unique()[1]))
                except: reqnum = None
            if args.jira_parent:
                jira_parent = args.jira_parent
            else:
                try:
                    jira_parent = args.nite_df.loc[index,('jira_parent')].unique()[1]
                except: jira_parent = None
            new_reqnum,new_jira_parent = jira_utils.create_ticket(args.jira_section,args.jira_user,
                                                  description=args.jira_description,
                                                  summary=summary,
                                                  ticket=reqnum,parent=jira_parent,
                                                  use_existing=True)
            # Update dataframe with reqnum, jira_id
            try: args.nite_df.loc[index,('reqnum')] = new_reqnum
            except: args.nite_df.insert(index[0],'reqnum',new_reqnum)
            try: args.nite_df.loc[index,('jira_parent')] = new_jira_parent
            except: args.nite_df.insert(index[0],'jira_parent',new_jira_parent) 

    # Render and write templates
    campaign_path = "pipelines/precal/%s/submitwcl" % args.campaign
    submit_template_path = os.path.join(campaign_path,"precal_submit_template.des")
    bash_template_path = os.path.join("scripts","submitme_template.sh")
    args.rendered_template_path = []
    for index,row in args.nite_df.iterrows():
        args.band,args.nite,args.reqnum args.dflat_list,args.bias_list= row['expnum'],row['band'],row['nite'],row['reqnum']
        output_name = "%s_%s_r%s_precal_rendered_template.des" % (expnum,band,reqnum)
        output_path = os.path.join(args.pipebox_work,output_name)
        # Writing template
        pipebox_utils.write_template(submit_template_path,output_path,args)

        if args.savefiles:
            # Current schema of writing dessubmit bash script 
            bash_script_name = "submitme_precal_%s_%s.sh" % (args.reqnum,args.target_site)
            bash_script_path= os.path.join(args.pipebox_work,bash_script_name)
            args.rendered_template_path.append(output_path)
        else:
            # If less than queue size submit nite
            if pipebox_utils.less_than_queue('precal',args.queue_size):
                pipebox_utils.submit_nite(output_name)

    if args.savefiles:
        pipebox_utils.write_template(bash_template_path,bash_script_path,args)
        os.chmod(bash_script_path, 0755)
        print "\n"
        print "# To submit files (from dessub/descmp1):\n"
        print "\t ssh dessub/descmp1"
        print "\t setup -v %s %s" % (args.eups_product,args.eups_version)
        print "\t %s\n" % bash_script_name

        # Print warning of Fermigrid credentials
        if args.target_site == 'fermigrid-sl6':
            print "# For FermiGrid please make sure your credentials are valid"
            print "\t setenv X509_USER_PROXY $HOME/.globus/osg/user.proxy"
            print "\t voms-proxy-info --all"
