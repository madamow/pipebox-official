#!/usr/bin/env python

import os,sys
from datetime import datetime
from argparse import ArgumentParser
import pandas as pd
from pipebox import pipebox_utils,jira_utils,query
from opstoolkit import common

def cmdline():
    # Create command line arguments
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--db_section',help = "e.g., db-desoper or db-destest")
    parser.add_argument('--calnite',help='bias/flat calibration nite/niterange,\
                                          i.e., 20151020 or 20151020t1030')
    parser.add_argument('--calrun',help='bias/flat calibration run, i.e., r1948p03')
    parser.add_argument('--caltag',help='Tag in OPS_PROCTAG to use if you calnite/calrun not specified')
    parser.add_argument('--target_site',help='Computing node, i.e., fermigrid-sl6')
    parser.add_argument('--http_section',help='')
    parser.add_argument('--archive_name',help='')
    parser.add_argument('--schema',help='')
    parser.add_argument('--jira_parent',help='')
    parser.add_argument('--jira_description',help='')
    parser.add_argument('--jira_project',default='DESOPS',help='')
    parser.add_argument('--jira_summary',help='Title of JIRA ticket')
    parser.add_argument('--jira_user',default = os.environ['USER'],help='JIRA username')
    parser.add_argument('--jira_section',help='JIRA section in .desservices.ini file')
    parser.add_argument('--eups_product',help='')
    parser.add_argument('--eups_version',help='')
    parser.add_argument('--campaign',help='Used in archive dir, e.g., Y2T3')
    parser.add_argument('--project',default='ACT',help='')
    parser.add_argument('--queue_size',help='')
    parser.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit")
    parser.add_argument('--savefiles',action='store_true',help='Saves submit files to submit later.')
    parser.add_argument('--expnum',help='A single expnum or comma-separated list of expnums')
    parser.add_argument('--list',help='File of line-separated expnums')
    parser.add_argument('--csv',help='')
    parser.add_argument('--delimiter',default=',',help='csv')
    parser.add_argument('--ccdnum',default=pipebox_utils.ALL_CCDS,help='')
    parser.add_argument('--reqnum',help='')
    parser.add_argument('--nite',help='')
    parser.add_argument('--paramfile',help='')
    parser.add_argument('--exptag',help='Grab all expnums with given tag in exposuretag table')
    parser.add_argument('--labels',help='')
    parser.add_argument('--epoch',help='SVE1,SVE2,Y1E1,Y1E2,Y2E1,Y2E2...')
    parser.add_argument('--campaignlib',help='Campaign where templates are stored, e.g., Y2A1dev')

    args = parser.parse_args()
    return args

if __name__ == "__main__":
  
    args = cmdline()

    args.submittime = datetime.now()
    
    if args.paramfile:
        args = pipebox_utils.update_from_param_file(args)
        args = pipebox_utils.replace_none_str(args)
    
    try:
        args.pipebox_work = os.environ['PIPEBOX_WORK']
        args.pipebox_dir = os.environ['PIPEBOX_DIR']
    except:
        print "must declare $PIPEBOX_WORK"
        sys.exit(1)    

    # For each use-case create exposures list and exposure dataframe
    cur = query.FinalCut(args.db_section)

    if args.exptag:
        args.exposure_list = cur.get_expnums_from_tag(args.exptag)
        args.exposure_df = pd.DataFrame(args.exposure_list,columns=['expnum'])
    elif args.expnum: 
        args.exposure_list = args.expnum.split(',')
        args.exposure_df = pd.DataFrame(args.exposure_list,columns=['expnum'])
    elif args.list: 
        with open(args.list) as listfile:
            args.exposure_list = listfile.read().splitlines()
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
            args.exposure_df.loc[index,'reqnum'] = new_reqnum
        try:
            args.exposure_df.loc[index,('jira_parent')] = new_jira_parent
        except: 
            args.exposure_df.insert(len(args.exposure_df.columns),'jira_parent',None)
            args.exposure_df.loc[index,'jira_parent'] = new_jira_parent
    
    # Render and write templates
    campaign_path = "pipelines/finalcut/%s/submitwcl" % args.campaignlib
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
        output_name = "%s_%s_r%s_finalcut_rendered_template.des" % (args.expnum,args.band,args.reqnum)
        output_path = os.path.join(args.pipebox_work,output_name)
        # Writing template
        pipebox_utils.write_template(submit_template_path,output_path,args)

        if args.savefiles:
            # Current schema of writing dessubmit bash script 
            if reqnum_count > 1:
                bash_script_name = "submitme_%s_%s.sh" % (datetime.now().strftime('%Y-%m-%dT%H:%M'),args.target_site)
            else:
                bash_script_name = "submitme_%s_%s.sh" % (args.reqnum,args.target_site)
            bash_script_path= os.path.join(args.pipebox_work,bash_script_name)
            args.rendered_template_path.append(output_path)
        else:
            # If less than queue size submit exposure
            if pipebox_utils.less_than_queue('finalcut',args.queue_size):
                pipebox_utils.submit_exposure(output_name)

    if args.savefiles:
        # Writing bash submit scripts
        pipebox_utils.write_template(bash_template_path,bash_script_path,args)
        os.chmod(bash_script_path, 0755)
        pipebox_utils.print_submit_info('finalcut',site=args.target_site,
                                                   eups_product=args.eups_product,
                                                   eups_version=args.eups_version,
                                                   submit_file=bash_script_path)
