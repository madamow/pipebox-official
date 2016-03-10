import os
import sys
import datetime
import time
import pandas as pd
import numpy as np
import string
from pipebox import pipequery,pipeargs,pipeutils,jira_utils,nitelycal_lib

class PipeLine(object):
    # Setup key arguments and environment here instead of write_*.sh
    if not os.getenv('PIPEBOX_WORK') or not os.getenv('PIPEBOX_DIR'):
        print "Please set $PIPEBOX_DIR & $PIPEBOX_WORK in your environment!"
        sys.exit(1)
    else:
        pipebox_work = os.environ['PIPEBOX_WORK']
        pipebox_dir = os.environ['PIPEBOX_DIR']

    def update_args(self,args):
        """ Update pipeline's arguments with template paths"""

        args.pipebox_dir,args.pipebox_work=self.pipebox_dir,self.pipebox_work

        if self.args.ignore_jira:
            if not self.args.reqnum or not self.args.jira_parent:
                print "Must specify both --reqnum and --jira_parent to avoid using JIRA!"
                sys.exit(1)
        
        # Format RA and Dec if given
        if self.args.RA or self.args.Dec:
            if not (self.args.RA and self.args.Dec):
                print "Must specify both RA and Dec."
                sys.exit(1)

        for a in ['RA','Dec','niterange','eups_stack']:
            if getattr(args,a):
                if len(getattr(args,a)[0]) > 1:
                    setattr(args,a,getattr(args,a)[0])
                else:
                    setattr(args,a,getattr(args,a)[0][0].split())
        
        # Setting niterange
        if self.args.nite and self.args.niterange:
            print "Warning: Both nite and niterange are specified. Only nite will be used."
        if self.args.nite:
            self.args.nitelist = self.args.nite.strip().split(',')
        if self.args.niterange:
            self.args.nitelist = pipeutils.create_nitelist(self.args.niterange[0],self.args.niterange[1]) 

        # If ngix -- cycle trough server's list
        if self.args.nginx:
            self.args.nginx_server = pipeutils.cycle_list_index(index,['desnginx', 'dessub'])
    
        if args.configfile: 
            if '/' in args.configfile:
                pass
            else:
                args.configfile = os.path.join(os.getcwd(),args.configfile) 

        # Checking if exclude list is a comma-separated list of line-separated file
        if args.exclude_list:
            exclude_file = os.path.isfile(args.exclude_list)
            if exclude_file:
                args.exclude_list =  list(pipeutils.read_file(args.exclude_list))
            else:
                try: 
                    dig = int(args.exclude_list[0])
                    args.exclude_list = args.exclude_list.strip().split(',')
                except IOError:
                   print "{0} does not exist!".format(args.exclude_list)
       
        # Setting template path(s) 
        campaign_path = "pipelines/%s/%s/submitwcl" % (args.pipeline,args.campaign)
        if args.template_name:
            args.submit_template_path = os.path.join(campaign_path,args.template_name)
        else:
            args.submit_template_path = os.path.join(campaign_path,
                                        "{0}_submit_template.des".format(args.pipeline))
        args.rendered_template_path = []
        
    def make_templates(self,columns=[],groupby=None):
        """ Loop through dataframe and write submitfile for each exposures"""
        # Updating args for each row
        for name, group in self.args.dataframe.groupby(by=groupby):
            # Setting jira parameters
            self.args.reqnum, self.args.jira_parent= group['reqnum'].unique()[0],group['jira_parent'].unique()[0]
            # Finding epoch of given data
            if self.args.epoch:
                self.args.epoch_name = self.args.epoch
            else:
                if self.args.pipeline == 'widefield':
                    first_expnum = group['expnum'].unique()[0]
                else:
                    first_expnum = group['first_exp'].unique()[0]
                self.args.epoch_name = self.args.cur.find_epoch(first_expnum)
            # Adding column values to args
            for c in columns:
                setattr(self.args,c, group[c].unique()[0])
            # Making output directories and filenames
            if self.args.out:
                if not os.path.isdir(self.args.out):
                    os.makedirs(self.args.out)
                self.args.output_dir = self.args.out
            else:
                req_dir = 'r%s' % self.args.reqnum
                self.args.output_dir = os.path.join(self.args.pipebox_work,req_dir)
                if not os.path.isdir(self.args.output_dir):
                    os.makedirs(self.args.output_dir)
            # Creating output name
            output_name_suffix = "r%s_%s_%s_rendered_template.des" % \
                                (self.args.reqnum,self.args.target_site,self.args.pipeline)
            
            str_base = []
            for i,k in enumerate(self.args.output_name_keys):
                st = "%s" % getattr(self.args,k)
                str_base.append(st)
            output_name_base = '_'.join(str_base)
            output_name = output_name_base + '_' + output_name_suffix
            output_path = os.path.join(self.args.output_dir,output_name)
            self.args.submitfile = output_path 
            # Writing template
            if self.args.ignore_processed:
                if self.args.cur.check_submitted(self.args.expnum,self.args.reqnum):
                    continue
                else:
                    pipeutils.write_template(self.args.submit_template_path,output_path,self.args)
                    self.args.rendered_template_path.append(output_path)
                    if not self.args.savefiles:
                        super(self.__class__,self).submit(self.args)
            else: 
                pipeutils.write_template(self.args.submit_template_path,output_path,self.args)
                self.args.rendered_template_path.append(output_path)
                if not self.args.savefiles:
                    super(self.__class__,self).submit(self.args)
                 
                    # Make comment in JIRA
                    if not self.args.ignore_jira:
                        con=jira_utils.get_con(self.args.jira_section)
                        if not jira_utils.does_comment_exist(con,reqnum=self.args.reqnum):
                            jira_utils.make_comment(con,datetime=datetime.datetime.now(),reqnum=self.args.reqnum)

        if self.args.auto:
            if not self.args.rendered_template_path: 
                print "No new data found on %s..." % datetime.datetime.now()
            else: print "%s data found on %s..." % (len(self.args.rendered_template_path),
                                                         datetime.datetime.now())

        if self.args.savefiles:
            super(self.__class__,self).save(self.args)

    
    def ticket(self,args,groupby='nite'):
        """ Create  JIRA ticket for each group specified"""
        try:
            args.dataframe
        except:
            print "Must specify input data!"
            sys.exit(1)

        group = args.dataframe.groupby(by=[groupby])
        for name,vals in group:
            # create JIRA ticket per nite and add jira_id,reqnum to dataframe
            index = args.dataframe[args.dataframe[groupby] == name].index
          
            if args.jira_summary:
                jira_summary = args.jira_summary 
            else:
                jira_summary = str(name)
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
                args.dataframe.loc[index,('reqnum')] = new_reqnum
            except:
                args.dataframe.insert(len(args.dataframe.columns),'reqnum',None)
                args.dataframe.loc[index,'reqnum'] = new_reqnum
            try:
                args.dataframe.loc[index,('jira_parent')] = new_jira_parent
            except:
                args.dataframe.insert(len(args.dataframe.columns),'jira_parent',None)
                args.dataframe.loc[index,'jira_parent'] = new_jira_parent

        return args.dataframe

    def auto(self,args):
        """ Sets up cronfile if necessary and runs auto-submit routine for given pipeline"""
        # Set crontab path
        cron_template_path = os.path.join('scripts',
                                    "cron_{0}_autosubmit_template.sh".format(args.pipeline))
        cron_submit_path = os.path.join(args.pipebox_work,
                                    "cron_{0}_autosubmit_rendered_template.sh".format(args.pipeline))
        if args.savefiles:
            # Writing template
            pipeutils.write_template(cron_template_path,cron_submit_path,args)
            os.chmod(cron_submit_path, 0755)
            pipeutils.print_cron_info(args.pipeline,site=args.target_site,
                                                   pipebox_work=args.pipebox_work,
                                                   cron_path=cron_submit_path)

    def save(self,args):
        bash_template_path = os.path.join("scripts","submitme_template.sh")
        # Writing bash submit scripts
        reqnum_count = len(args.dataframe.groupby(by=['reqnum']))
        if reqnum_count > 1:
            bash_script_name = "submitme_%s_%s.sh" % (datetime.datetime.now().strftime('%Y-%m-%dT%H:%M'),
                                                          args.target_site)
        else:
            bash_script_name = "submitme_%s_%s.sh" % (args.reqnum,args.target_site)
        bash_script_path= os.path.join(args.output_dir,bash_script_name)

        pipeutils.write_template(bash_template_path,bash_script_path,args)
        os.chmod(bash_script_path, 0755)
        pipeutils.print_submit_info(args.pipeline,site=args.target_site,
                                               eups_stack=args.eups_stack,
                                               submit_file=bash_script_path) 

    def submit(self,args):
        # If less than queue size submit exposure
        if args.total_queue:
            desstat_user = None
        else:
            desstat_user = args.jira_user
        if pipeutils.less_than_queue(pipeline=args.desstat_pipeline,
                                     user=desstat_user,queue_size=args.queue_size):
            args.unitname,args.attnum = pipeutils.submit_command(args.submitfile,wait=float(args.wait))
        else:
            while not pipeutils.less_than_queue(pipeline=args.desstat_pipeline,
                                                user=desstat_user,queue_size=args.queue_size):
                time.sleep(30)
            else:
                args.unitname,args.attnum = pipeutils.submit_command(args.submitfile,wait=float(args.wait))
    
        # Update submitfile name with attnum
        pipeutils.rename_file(args)

class SuperNova(PipeLine):

    def __init__(self):
        """ Initialize arguments and configure"""

        # Setting global parameters
        self.args = pipeargs.SupernovaArgs().cmdline()
        self.args.pipebox_dir,self.args.pipebox_work=self.pipebox_dir,self.pipebox_work
        self.args.pipeline = self.args.desstat_pipeline = "sne"
        self.args.output_name_keys = ['nite','field','band']

        super(SuperNova,self).update_args(self.args)         
        self.args.cur = pipequery.SupernovaQuery(self.args.db_section)
        
        # Creating dataframe from exposures 
#       I don't think we want this for SN
#        if self.args.exptag:
#            self.args.exposure_list = self.args.cur.get_expnums_from_tag(self.args.exptag)
#            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        if self.args.nite:
            self.args.triplet_list = self.args.cur.get_triplets_from_nite(self.args.nitelist)
            self.args.dataframe = pd.DataFrame(self.args.triplet_list,columns=['nite','field','band'])
        elif self.args.triplet:
            self.args.triplet_list = np.array(self.args.triplet.split(',')).reshape([-1,3])
            self.args.dataframe = pd.DataFrame(self.args.triplet_list,columns=['nite','field','band'])
        elif self.args.list:
            self.args.triplet_list = np.array(string.join(pipeutils.read_file(self.args.list)).split(' ')).reshape([-1,3])
            print self.args.triplet_list
            self.args.dataframe = pd.DataFrame(self.args.triplet_list,columns=['nite','field','band'])
        elif self.args.csv:
            self.args.dataframe = pd.read_csv(self.args.csv,sep=self.args.delimiter)
            self.args.dataframe.columns = [col.lower() for col in self.args.dataframe.columns]
            self.args.triplet_list = np.array(self.args.dataframe[['nite','field','band']].values)
        nrows=len(self.args.dataframe)
        self.args.dataframe['exp_nums']=np.zeros(nrows, dtype=str)
        self.args.dataframe['first_exp']=np.zeros(nrows, dtype=str)
        self.args.dataframe['single']=np.ones(nrows, dtype=bool)
        self.args.dataframe['fringe']=np.zeros(nrows, dtype=bool)
        self.args.dataframe['ccdlist']=np.zeros(nrows, dtype=str)
        self.args.dataframe['seqnum']=np.ones(nrows, dtype=int)
        # Update dataframe for each exposure and add expnums,firstexp if not exists
#        try:
#            self.args.dataframe = self.args.cur.update_df(self.args.dataframe)
#            self.args.dataframe = self.args.dataframe.fillna(False)
#        except: 
#            pass
        self.args.dataframe = self.args.cur.update_df(self.args.dataframe)
        self.args.dataframe = self.args.dataframe.fillna(False)

class WideField(PipeLine):

    def __init__(self):
        """ Initialize arguments and configure"""
        # Setting global parameters
        self.args = pipeargs.WidefieldArgs().cmdline()
        self.args.pipeline = "widefield"
        if 'N' in self.args.campaign:
            self.args.desstat_pipeline = "firstcut"
        else:
            self.args.desstat_pipeline = "finalcut" 

        super(WideField,self).update_args(self.args)
        self.args.output_name_keys = ['nite','expnum','band']
       
        self.args.cur = pipequery.WidefieldQuery(self.args.db_section)
        
        self.args.propid = self.args.propid.strip().split(',')
        self.args.program = self.args.program.strip().split(',')
 
        # If auto-submit mode on
        if self.args.auto:
            self.args.ignore_processed=True
            pipeutils.stop_if_already_running('submit_{0}.py'.format(self.args.pipeline))
            
            self.args.nite = self.args.cur.get_max_nite(propid=self.args.propid,
                                                        program=self.args.program,
                                                        process_all=self.args.process_all)[1]
            if not self.args.calnite:
                precal = self.args.cur.find_precal(self.args.nite,threshold=7,override=True,
                                                   tag=self.args.caltag)
                self.args.calnite,self.args.calrun = precal[0],precal[1]
        
        
        # Creating dataframe from exposures 
        if self.args.resubmit_failed:
            self.args.ignore_processed=False
            self.args.exposure_list = self.args.cur.get_failed_expnums(self.args.reqnum)
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.exptag:
            self.args.exposure_list = self.args.cur.get_expnums_from_tag(self.args.exptag)
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum','tag']).sort(columns=['tag','expnum'],ascending=True)
        elif self.args.expnum:
            self.args.exposure_list = self.args.expnum.split(',')
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.nite or self.args.niterange:
            exposures = self.args.cur.get_expnums_from_nites(self.args.nitelist,propid=self.args.propid,
                                program=self.args.program,process_all=self.args.process_all)
            if not exposures:
                print "No exposures found for given nite. Please check nite."
                sys.exit(1)
            self.args.exposure_list = [expnum for expnum in exposures]
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.list:
            self.args.exposure_list = list(pipeutils.read_file(self.args.list))
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.csv:
            self.args.dataframe = pd.read_csv(self.args.csv,sep=self.args.delimiter)
            self.args.dataframe.columns = [col.lower() for col in self.args.dataframe.columns]
            self.args.exposure_list = list(self.args.dataframe['expnum'].values)
        elif self.args.RA and self.args.Dec:
            self.args.exposure_list = self.args.cur.get_expnums_from_radec(self.args.RA, self.args.Dec)
            self.args.dataframe = pd.DataFrame(self.args.exposure_list, columns=['expnum'])
        # Remove unwanted exposures 
        if self.args.exclude_list:
            self.args.dataframe = self.args.dataframe[~self.args.dataframe.expnum.isin(self.args.exclude_list)]
        # Update dataframe for each exposure and add band,nite if not exists
        try:
            self.args.dataframe = self.args.cur.update_df(self.args.dataframe)
            self.args.dataframe = self.args.dataframe.fillna(False)
        except: 
            pass

class NitelyCal(PipeLine):

    def __init__(self):
        """ Initialize arguments and configure"""
        self.args = pipeargs.NitelycalArgs().cmdline()
        self.args.pipeline = "nitelycal"

        super(NitelyCal,self).update_args(self.args)
        self.args.cur = pipequery.NitelycalQuery(self.args.db_section)
        self.args.bands = self.args.bands.strip().split(',') 
        self.args.output_name_keys = ['niterange']
        # If auto-submit mode on
        if self.args.auto:
            self.args.ignore_processed=True
            pipeutils.stop_if_already_running('submit_{0}.py'.format(self.args.pipeline))
            self.args.nite = self.args.cur.get_max_nite()[1] 

        if (self.args.maxnite or self.args.minnite) and self.args.niterange:
            print 'Warning: if specifying minnite and/or maxnite, do not use niterange' 
            sys.exit()

        # Create remaining list of nites if necessary
        self.args.calculate_nites = False
        if self.args.nite:
            if len(self.args.nitelist) == 1:
                self.args.calculate_nites = True
        if self.args.maxnite and not self.args.minnite:
            self.args.nitelist = self.args.maxnite.split(',')
            self.args.calculate_nites = True
        elif self.args.minnite and not self.args.maxnite:
            self.args.nitelist = self.args.minnite.split(',')
            self.args.calculate_nites = True
        elif self.args.minnite and self.args.maxnite:
            self.args.nitelist = pipeutils.create_nitelist(self.args.minnite,self.args.maxnite)

        # For each use-case create bias/flat list and dataframe
        if self.args.biaslist and self.args.flatlist:
            # create biaslist from file
            self.args.bias_list = list(pipeutils.read_file(self.args.biaslist))
            self.args.bias_df = pd.DataFrame(self.args.bias_list,columns=['expnum'])

            # create flatlist from file
            self.args.flat_list = list(pipeutils.read_file(self.args.flatlist))
            self.args.flat_df = pd.DataFrame(self.args.flat_list,columns=['expnum'])

            self.args.dataframe = pd.concat([self.args.flat_df,self.args.bias_df],ignore_index=True)
            self.args.cur.update_df(self.args.dataframe)

        if self.args.csv:
            self.args.dataframe = pd.read_csv(self.args.csv,sep=self.args.delimiter)
            self.args.dataframe.columns = [col.lower() for col in self.args.dataframe.columns]
            self.args.cur.update_df(self.args.dataframe)
            self.args.bias_list,self.args.flat_list = nitelycal_lib.create_lists(self.args.dataframe)
        else:
            cal_query = self.args.cur.get_cals(self.args.nitelist)
            self.args.dataframe = nitelycal_lib.create_clean_df(cal_query)
            if self.args.combine:
                if self.args.calculate_nites:
                    _,not_enough_exp = nitelycal_lib.trim_excess_exposures(self.args.dataframe,
                                                                           self.args.bands,
                                                                           k=self.args.max_num)
                    # If not enough exposures per band +/- one day until enough are found
                    while not_enough_exp: 
                        oneday = datetime.timedelta(days=1)
                        low_nite = datetime.datetime.strptime(self.args.nitelist[0],'%Y%m%d').date()
                        high_nite = datetime.datetime.strptime(self.args.nitelist[-1],'%Y%m%d').date()
                        if self.args.nite:
                            self.args.nitelist.insert(0,str(low_nite-oneday).replace('-',''))
                            self.args.nitelist.append(str(high_nite+oneday).replace('-',''))
                        if self.args.maxnite and not self.args.minnite:
                            self.args.nitelist.insert(0,str(low_nite-oneday).replace('-',''))
                        if self.args.minnite and not self.args.maxnite:
                            self.args.nitelist.append(str(high_nite+oneday).replace('-',''))

                        cal_query = self.args.cur.get_cals(self.args.nitelist)            
                        self.args.dataframe = nitelycal_lib.create_clean_df(cal_query)
                        _,not_enough_exp = nitelycal_lib.trim_excess_exposures(self.args.dataframe,
                                                                               self.args.bands,
                                                                               k=self.args.max_num)
                self.args.niterange = str(self.args.nitelist[0]) + 't' + str(self.args.nitelist[-1])[4:]

            # Removing bands that are not specified
            self.args.dataframe = self.args.dataframe[self.args.dataframe.band.isin(self.args.bands)\
                                                      |self.args.dataframe.obstype.isin(['zero'])]

        if self.args.combine:
            self.args.desstat_pipeline = "supercal"
            self.args.dataframe['niterange'] = self.args.niterange
            self.args.bias_list,self.args.flat_list = nitelycal_lib.create_lists(self.args.dataframe)

        else:
            self.args.desstat_pipeline = "precal"
            for index,row in self.args.dataframe.iterrows():
                try:
                    self.args.dataframe.loc[index,('niterange')] = str(row['nite'])
                except:
                    self.args.dataframe.insert(len(self.args.dataframe.columns),'niterange',None)
                    self.args.dataframe.loc[index,('niterange')] = str(row['nite'])

        # Update dataframe with lists
        try:
            self.args.dataframe.insert(len(self.args.dataframe),'bias_list',None)
            self.args.dataframe.insert(len(self.args.dataframe),'bias_list',None)
        except: pass

        for niterange,group in self.args.dataframe.groupby(by=['niterange']):
            index = group.index
            if not self.args.combine:
                # Append bias/flat lists to dataframe
                self.args.bias_list,self.args.flat_list = nitelycal_lib.create_lists(group)

            self.args.first_exp = self.args.flat_list[0]
            self.args.bias_list = ','.join(str(i) for i in self.args.bias_list)
            self.args.flat_list = ','.join(str(i) for i in self.args.flat_list)
            self.args.dataframe.loc[index,'flat_list'] = self.args.flat_list
            self.args.dataframe.loc[index,'bias_list'] = self.args.bias_list
            self.args.dataframe.loc[index,'first_exp'] = self.args.first_exp
        
        # Exit if there are not at least 5 exposures per band
        if self.args.auto:
            nitelycal_lib.is_count_by_band(self.args.dataframe,bands_to_process=self.args.bands,
                                           min_per_sequence=self.args.min_per_sequence)
       
        if self.args.combine: 
            self.args.dataframe,not_enough_exp = nitelycal_lib.trim_excess_exposures(self.args.dataframe,
                                                                                     self.args.bands,
                                                                                     k=self.args.max_num,
                                                                                     verbose= True)
        
        self.args.dataframe,self.args.nitelist = nitelycal_lib.find_no_data(self.args.dataframe,self.args.nitelist)
        
        if self.args.count:
            print "Data found in database:"
            self.args.cur.count_by_band(self.args.nitelist)
            print "\nData to be processed: %s" % ','.join(self.args.nitelist)
            nitelycal_lib.final_count_by_band(self.args.dataframe)
            sys.exit(0)

class HostName(PipeLine):
    
    def __init__(self):
        self.args = pipeargs.HostnameArgs().cmdline()
        self.args.pipeline = self.args.desstat_pipeline = "hostname"
        self.args.pipebox_work,self.args.pipebox_dir = self.pipebox_work,self.pipebox_dir
        self.args.submit_template_path = os.path.join("pipelines/{0}".format(self.args.pipeline),
                                                   "{0}_template.des".format(self.args.pipeline)) 
        if len(self.args.eups_stack[0])>1:
            self.args.eups_stack = self.args.eups_stack[0]
        else:
            self.args.eups_stack = self.args.eups_stack[0][0].split()

    def ticket(self):
        # Create JIRA ticket
        new_reqnum,new_jira_parent = jira_utils.create_ticket(self.args.jira_section,
                                          self.args.jira_user,
                                          description=self.args.jira_description,
                                          summary=self.args.jira_summary,
                                          ticket=self.args.reqnum,parent=self.args.jira_parent,
                                          use_existing=True)

        self.args.reqnum,self.args.jira_parent = new_reqnum,new_jira_parent
    
    def make_templates(self):
        req_dir = 'r%s' % self.args.reqnum
        self.args.output_dir = os.path.join(self.args.pipebox_work,req_dir)
        if not os.path.isdir(self.args.output_dir):
            os.makedirs(self.args.output_dir)
        output_name = "%s_r%s_%s_rendered_template.des" % (self.args.pipeline,self.args.reqnum,self.args.target_site)
        output_path = os.path.join(self.args.output_dir,output_name)
        self.args.rendered_template_path = []
        self.args.rendered_template_path.append(output_path)
        pipeutils.write_template(self.args.submit_template_path,output_path,self.args)
    
    def submit_or_save(self):
        if self.args.savefiles:
            bash_template_path = os.path.join("scripts","submitme_template.sh")
            # Current schema of writing dessubmit bash script
            bash_script_name = "submitme_%s_%s_%s.sh" % (self.args.pipeline,self.args.reqnum,
                                                         self.args.target_site)
            bash_script_path= os.path.join(self.args.output_dir,bash_script_name)
            # Write bash script
            pipeutils.write_template(bash_template_path,bash_script_path,self.args)
            os.chmod(bash_script_path, 0755)
            pipeutils.print_submit_info(self.args.pipeline,site=self.args.target_site,
                                        eups_stack=self.args.eups_stack,
                                        submit_file=bash_script_path)

        else:
            for submitfile in self.args.rendered_template_path:
                self.args.submitfile = submitfile
                self.args.unitname,self.args.attnum = pipeutils.submit_command(submitfile,wait=float(self.args.wait))
                # Adding attnum to output_name
                pipeutils.rename_file(self.args)
