import os
import sys
import pandas as pd
from pipebox import pipequery,pipeargs,jira_utils
from autosubmit import firstcut,nitelycal

class PipeLine(object):
    # Setup key arguments and environment here instead of write_*.sh
    if not os.getenv('PIPEBOX_WORK') or not os.getenv('PIPEBOX_DIR'):
        print "Please set $PIPEBOX_DIR & $PIPEBOX_WORK in your environment!"
        sys.exit(1)
    else:
        pipebox_work = os.environ['PIPEBOX_WORK']
        pipebox_dir = os.environ['PIPEBOX_DIR']

    def ticket(self,args,groupby='nite'):
        """ Create  JIRA ticket for each group specified"""
        try:
            args.dataframe
        except:
            print "Must specify args.dataframe!"
            sys.exit(1)

        group = args.dataframe.groupby(by=[groupby])
        for name,vals in group:
            # create JIRA ticket per nite and add jira_id,reqnum to dataframe
            index = args.dataframe[args.dataframe[groupby] == name].index

            if not args.jira_summary:
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

class WideField(PipeLine):

    def __init__(self):
        self.args = pipeargs.WidefieldArgs().cmdline()
        self.cur = pipequery.WidefieldQuery(self.args.db_section)

        if self.args.exptag:
            self.args.exposure_list = self.cur.get_expnums_from_tag(self.args.exptag)
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.expnum:
            self.args.exposure_list = self.args.expnum.split(',')
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.list:
            self.args.exposure_list = list(pipeutils.read_file(args.list))
            self.args.dataframe = pd.DataFrame(self.args.exposure_list,columns=['expnum'])
        elif self.args.csv:
            self.args.dataframe = pd.read_csv(self.args.csv,sep=self.args.delimiter)
            self.args.dataframe.columns = [col.lower() for col in self.args.dataframe.columns]
            self.args.exposure_list = list(self.args.dataframe['expnum'].values)

        # Update dataframe for each exposure and add band,nite if not exists
        try:
            self.args.dataframe = self.cur.update_df(self.args.dataframe)
            self.args.dataframe = self.args.dataframe.fillna(False)
        except: 
            pass

    def auto(self):
        firstcut.run(self.args,self.cur)
   
class NitelyCal(PipeLine):

    def __init__(self):
        self.args = pipeargs.NitelycalArgs().cmdline()
        self.cur = pipequery.NitelycalQuery(self.args.db_section)
    
    def auto(self):
        nitelycal.run(self.args,self.cur)

class HostName(PipeLine):
    
    def __init__(self):
        self.args = pipeargs.PipeArgs().cmdline()
