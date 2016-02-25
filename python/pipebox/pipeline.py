import os
import sys
from datetime import datetime
import time
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
from pipebox import pipequery,pipeargs,pipeutils,jira_utils,nitelycal_lib
from autosubmit import nitelycal

class PipeLine(object):
    # Setup key arguments and environment here instead of write_*.sh
    if not os.getenv('PIPEBOX_WORK') or not os.getenv('PIPEBOX_DIR'):
        print "Please set $PIPEBOX_DIR & $PIPEBOX_WORK in your environment!"
        sys.exit(1)
    else:
        pipebox_work = os.environ['PIPEBOX_WORK']
        pipebox_dir = os.environ['PIPEBOX_DIR']

    __metaclass__ = ABCMeta
    @abstractmethod
    def make_templates():
        """Must be defined for each pipeline subclass"""
        pass
    
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
        else:
            # Run autosubmit code directly
            # Will run once, but if put in crontab will run however you specify in cron

            # Kill current process if cron is running from last execution
            pipeutils.stop_if_already_running('submit_{0}.py'.format(args.pipeline))
            pipeline = __import__("autosubmit")
            getattr(pipeline,args.pipeline).run(args)

    def set_paths(self,args):
        """ Update pipeline's arguments with template paths"""
        # Render and write templates
        campaign_path = "pipelines/%s/%s/submitwcl" % (args.pipeline,args.campaignlib)
        if args.template_name:
            args.submit_template_path = os.path.join(campaign_path,args.template_name)
        else:
            args.submit_template_path = os.path.join(campaign_path,
                                        "{0}_submit_template.des".format(args.pipeline))
        args.rendered_template_path = []

    def save(self,args):
        bash_template_path = os.path.join("scripts","submitme_template.sh")
        # Writing bash submit scripts
        reqnum_count = len(args.dataframe.groupby(by=['reqnum']))
        if reqnum_count > 1:
            bash_script_name = "submitme_%s_%s.sh" % (datetime.now().strftime('%Y-%m-%dT%H:%M'),
                                                          args.target_site)
        else:
            bash_script_name = "submitme_%s_%s.sh" % (args.reqnum,args.target_site)
        bash_script_path= os.path.join(args.output_dir,bash_script_name)

        pipeutils.write_template(bash_template_path,bash_script_path,args)
        os.chmod(bash_script_path, 0755)
        pipeutils.print_submit_info(args.pipeline,site=args.target_site,
                                               eups_product=args.eups_product,
                                               eups_version=args.eups_version,
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
        self.args.pipeline = "sne"

        super(SuperNova,self).set_paths(self.args)         
        self.args.cur = pipequery.SupernovaQuery(self.args.db_section)
        
        # If ngix -- cycle trough server's list
        if self.args.nginx:
            self.args.nginx_server = pipeutils.cycle_list_index(index,['desnginx', 'dessub'])

        # Creating dataframe from exposures 
#       I don't think we want this for SN
#        if self.args.exptag:
#            self.args.exposure_list = self.args.cur.get_expnums_from_tag(self.args.exptag)
#            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.triplet:
            self.args.triplet_list = np.array(self.args.triplet.split(',')).reshape([-1,3])
            self.args.dataframe = pd.DataFrame(self.args.triplet_list,columns=['nite','field','band'])
        elif self.args.list:
            self.args.triplet_list = list(pipeutils.read_file(self.args.list))
            self.args.dataframe = pd.DataFrame(self.args.triplet_list,columns=['nite','field','band'])
        elif self.args.csv:
            self.args.dataframe = pd.read_csv(self.args.csv,sep=self.args.delimiter)
            self.args.dataframe.columns = [col.lower() for col in self.args.dataframe.columns]
            self.args.triplet_list = np.array(self.args.dataframe[['nite','field','band']].values)
        nrows=len(self.args.triplet.split(','))/3
        self.args.dataframe['exp_nums']=np.zeros(nrows, dtype=str)
        self.args.dataframe['first_exp']=np.zeros(nrows, dtype=str)
        self.args.dataframe['single']=np.ones(nrows, dtype=bool)
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

    def make_templates(self):
        """ Loop through dataframe and write submitfile for each exposures"""
        for index,row in self.args.dataframe.iterrows():
            self.args.expnums,self.args.band,self.args.nite,self.args.firstexp,self.args.field,self.args.single,self.args.ccdlist,self.args.seqnum = row['exp_nums'],row['band'],row['nite'],row['first_exp'],row['field'],row['single'],row['ccdlist'],row['seqnum']
            self.args.reqnum, self.args.jira_parent= int(row['reqnum']),row['jira_parent']
#            if self.args.epoch:
#                self.args.epoch_name = self.args.epoch
#            else:
#                self.args.epoch_name = self.args.cur.find_epoch(int(row['first_exp']))
            # Making output directories and filenames
            req_dir = 'r%s' % self.args.reqnum
            self.args.output_dir = os.path.join(self.args.pipebox_work,req_dir)
            if not os.path.isdir(self.args.output_dir):
                os.makedirs(self.args.output_dir)
            output_name = "%s_%s_%s_r%s_%s_supernova_rendered_template.des" % \
                        (self.args.nite,self.args.field,self.args.band,self.args.reqnum,self.args.target_site)
            output_path = os.path.join(self.args.output_dir,output_name)
    
            # Writing template
            print self.args.submit_template_path, output_path
            pipeutils.write_template(self.args.submit_template_path,output_path,self.args)
            self.args.rendered_template_path.append(output_path)
            if not self.args.savefiles:
                super(SuperNova,self).submit(self.args)

        if self.args.savefiles:
            super(SuperNova,self).save(self.args)

class WideField(PipeLine):

    def __init__(self):
        """ Initialize arguments and configure"""

        # Setting global parameters
        self.args = pipeargs.WidefieldArgs().cmdline()
        self.args.pipebox_dir,self.args.pipebox_work=self.pipebox_dir,self.pipebox_work
        if self.args.ignore_jira:
            if not self.args.reqnum or not self.args.jira_parent:
                print "Must specify both --reqnum and --jira_parent to avoid using JIRA!"
                sys.exit(1)
        
        self.args.pipeline = "widefield"
        if 'N' in self.args.campaignlib:
            self.args.desstat_pipeline = "firstcut"
        else:
            self.args.desstat_pipeline = "finalcut" 

        super(WideField,self).set_paths(self.args)         
        self.args.cur = pipequery.WidefieldQuery(self.args.db_section)
        
        self.args.propid = self.args.propid.strip().split(',')
        self.args.program = self.args.program.strip().split(',')
        
        # If auto-submit mode on
        if self.args.auto:
            self.args.ignore_processed=True
            pipeutils.stop_if_already_running('submit_{0}.py'.format(self.args.pipeline))
            
            self.args.nite = self.args.cur.get_max_nite(propid=self.args.propid,program=self.args.program,
                                                   process_all=self.args.process_all)[1]
            if not self.args.calnite:
                precal = self.args.cur.find_precal(self.args.nite,threshold=7,override=True,
                                                   tag=self.args.caltag)
                self.args.calnite,self.args.calrun = precal[0],precal[1]
        
        # If ngix -- cycle trough server's list
        if self.args.nginx:
            self.args.nginx_server = pipeutils.cycle_list_index(index,['desnginx', 'dessub'])
        if self.args.RA or self.args.Dec:
            if not (self.args.RA and self.args.Dec):
                print "Must specify both RA and Dec."
                sys.exit(1)
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
        elif self.args.nite:
            exposures = self.args.cur.get_expnums_from_nite(self.args.nite,propid=self.args.propid,
                                program=self.args.program,process_all=self.args.process_all)
            if not exposures:
                print "No exposures found for given nite. Please check nite."
                sys.exit(1)
            self.args.exposure_list = [expnum for expnum,band in exposures]
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
	        self.args.exclude_list = self.args.exclude_list.strip().split(',')
	        self.args.dataframe = self.args.dataframe[~self.args.dataframe.expnum.isin(self.args.exclude_list)]
        # Update dataframe for each exposure and add band,nite if not exists
        try:
            self.args.dataframe = self.args.cur.update_df(self.args.dataframe)
            self.args.dataframe = self.args.dataframe.fillna(False)
        except: 
            pass

    def make_templates(self):
        """ Loop through dataframe and write submitfile for each exposures"""
        for index,row in self.args.dataframe.iterrows():
            self.args.expnum,self.args.band,self.args.nite = row['expnum'],row['band'],row['nite']
            self.args.reqnum, self.args.jira_parent= int(row['reqnum']),row['jira_parent']
            if self.args.epoch:
                self.args.epoch_name = self.args.epoch
            else:
                self.args.epoch_name = self.args.cur.find_epoch(row['expnum'])
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
            output_name = "%s_%s_r%s_%s_widefield_rendered_template.des" % \
                        (self.args.expnum,self.args.band,self.args.reqnum,self.args.target_site)
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
                        super(WideField,self).submit(self.args)
            else: 
                pipeutils.write_template(self.args.submit_template_path,output_path,self.args)
                self.args.rendered_template_path.append(output_path)

                if not self.args.savefiles:
                    super(WideField,self).submit(self.args)
                    
                    # Make comment in JIRA
                    if not self.args.ignore_jira:
                        con=jira_utils.get_con(self.args.jira_section)
                        if not jira_utils.does_comment_exist(con,reqnum=self.args.reqnum):
                            jira_utils.make_comment(con,datetime=datetime.now(),reqnum=self.args.reqnum)
        if self.args.auto:
            if not self.args.rendered_template_path: 
                print "No new exposures found on %s..." % datetime.now()
            else: print "%s exposures found on %s..." % (len(self.args.rendered_template_path),
                                                             datetime.now())

        if self.args.savefiles:
            super(WideField,self).save(self.args)

class NitelyCal(PipeLine):

    def __init__(self):
        """ Initialize arguments and configure"""
        self.args = pipeargs.NitelycalArgs().cmdline()
        self.args.pipebox_dir,self.args.pipebox_work=self.pipebox_dir,self.pipebox_work
        self.args.pipeline = "nitelycal"

        super(NitelyCal,self).set_paths(self.args)
        self.args.cur = pipequery.NitelycalQuery(self.args.db_section)
        
        # Create list of nites
        if self.args.nite:
            self.args.nitelist = self.args.nite.split(',')
        elif self.args.maxnite and self.args.minnite:
            self.args.nitelist = [str(x) for x in range(self.args.minnite,self.args.maxnite +1)]
        else:
            print "Please specify --nite or --maxnite and --minnite"
            sys.exit(1)

        self.args.niterange = str(self.args.minnite) + 't' + str(self.args.maxnite)[4:]

        if self.args.count:
            self.args.cur.count_by_band(self.args.nitelist)
            sys.exit(0)
    
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
            self.args.bias_list,self.args.flat_list = nitelycal_lib.create_lists(self.args.dataframe)

        if self.args.combine:
            self.args.desstat_pipeline = "supercal"
            self.args.dataframe['niterange'] = self.args.niterange
        else:
            self.args.desstat_pipeline = "precal"
            for index,row in self.args.dataframe.iterrows():
                try:
                    self.args.dataframe.loc[index,('niterange')] = str(row['nite'])
                except:
                    self.args.dataframe.insert(len(self.args.dataframe.columns),'niterange',None)
                    self.args.dataframe.loc[index,('niterange')] = str(row['nite'])

    def make_templates(self):
        """ Loop through dataframe and write submitfile for each nite/niterange"""
        for niterange,group in self.args.dataframe.groupby(by=['niterange']):
            if self.args.combine:
                self.args.nite = group['niterange'].unique()[0]
                self.args.bias_list = ','.join(str(i) for i in self.args.bias_list)
                self.args.flat_list = ','.join(str(i) for i in self.args.flat_list)
            else:
                self.args.nite = group['nite'].unique()[0]
                # Append bias/flat lists to dataframe
                bias_list,flat_list = nitelycal_lib.create_lists(group)
                self.args.bias_list = ','.join(str(i) for i in bias_list)
                self.args.flat_list = ','.join(str(i) for i in flat_list)
           
            if self.args.epoch:
                self.args.epoch_name = self.args.epoch
            else:
                self.args.epoch_name = self.args.cur.find_epoch(self.args.bias_list.split(',')[0])
            self.args.reqnum = group['reqnum'].unique()[0]
            self.args.jira_parent = group['jira_parent'].unique()[0]
    
            if self.args.out:
                if not os.path.isdir(self.args.out):
                    os.makedirs(self.args.out)
                self.args.output_dir = self.args.out
            else:
                #req_dir = 'r%s' % self.args.reqnum
                #output_dir = os.path.join(self.args.pipebox_work,req_dir)
                #if not os.path.isdir(output_dir):
                #    os.makedirs(output_dir)
                self.args.output_dir = self.args.pipebox_work
            output_name = "%s_r%s_nitelycal_rendered_template.des" % (self.args.nite,self.args.reqnum)
            output_path = os.path.join(self.args.output_dir,output_name)
            # Writing template
            pipeutils.write_template(self.args.submit_template_path,output_path,self.args)
            self.args.rendered_template_path.append(output_path)
            self.args.submitfile = output_path
            if not self.args.savefiles:
                super(NitelyCal,self).submit(self.args)
        if self.args.savefiles:
            super(NitelyCal,self).save(self.args)

class HostName(PipeLine):
    
    def __init__(self):
        self.args = pipeargs.HostnameArgs().cmdline()
        self.args.pipeline = self.args.desstat_pipeline = "hostname"
        self.args.pipebox_work,self.args.pipebox_dir = self.pipebox_work,self.pipebox_dir
        self.args.submit_template_path = os.path.join("pipelines/{0}".format(self.args.pipeline),
                                                   "{0}_template.des".format(self.args.pipeline)) 

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
                                          eups_product=self.args.eups_product,
                                          eups_version=self.args.eups_version,
                                          submit_file=bash_script_path)

        else:
            for submitfile in self.args.rendered_template_path:
                self.args.submitfile = submitfile
                self.args.unitname,self.args.attnum = pipeutils.submit_command(submitfile,wait=float(self.args.wait))
                # Adding attnum to output_name
                pipeutils.rename_file(self.args)
