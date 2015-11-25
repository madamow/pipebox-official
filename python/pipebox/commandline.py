import os
from argparse import ArgumentParser
from pipebox import jira_utils,pipebox_parse

class PipeArgs(object):

    @staticmethod 
    def argument_parser():
        # Create command line arguments
        parser = ArgumentParser(description=__doc__)
        # General arguments
        parser.add_argument('--db_section',help = "Database section in your \
                             .desservices.ini file, e.g., db-desoper or db-destest")
        parser.add_argument("--user", action="store", default=os.environ['USER'],
                            help="username that will submit")
        parser.add_argument('--paramfile',help='Key = Value file that can be used to replace command- \
                             line')
        parser.add_argument('--csv',help='CSV of exposures and information specified by user. If specified, \
                             code will use exposures in csv to submit jobs. Must also specify --delimiter')
        parser.add_argument('--delimiter',default=',',help='The delimiter if specifying csv and is not \
                             comma-separated')
        parser.add_argument('--campaignlib',help='Directory in pipebox where templates are stored, e.g., \
                             $PIPEBOX_DIR/templates/pipelines/finalcut/-->Y2A1dev<--')
        parser.add_argument('--savefiles',action='store_true',help='Saves submit files to submit later.')
        parser.add_argument('--queue_size',help='If set and savefiles is not specified, code \
                             will submit specified runs up until queue_size is reached. Code \
                             will wait until queue drops below limit to submit next job')
        parser.add_argument('--labels',help='Human-readable labels to "mark" a given processing attempt')
        parser.add_argument('--template_name',help='submitwcl template within pipeline/campaign')
        parser.add_argument('--configfile',help='Name of user cfg file')
        parser.add_argument('--auto',action='store_true',help='Will run autosubmit mode if specified')
           
        # Archive arguments
        parser.add_argument('--target_site',help='Computing node, i.e., fermigrid-sl6')
        parser.add_argument('--http_section',help='')
        parser.add_argument('--archive_name',help='Home archive to store products, e.g., \
                             desar2home,prodbeta,...')
        parser.add_argument('--campaign',help='Used in archive dir, e.g., Y2T3')
        parser.add_argument('--project',default='ACT',help='Archive directory where runs are \
                             stored, e.g., $ARCHIVE/-->ACT<--/finalcut/')
      
        # JIRA arguments
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
        parser.add_argument('--reqnum',help='Part of processing unique identifier. Tied to JIRA ticket \
                             number')
    
        # EUPS arguments
        parser.add_argument('--eups_product',help='EUPS production stack, e.g., finalcut')
        parser.add_argument('--eups_version',help='EUPS production stack version, e.g., Y2A1+1')
        
        # Science arguments
        parser.add_argument('--ccdnum',help='CCDs to be processed.')
        parser.add_argument('--nite',help='For auto mode: if specified will submit all exposures found \
                         from nite')

        # Transfers
        parser.add_argument('--nginx',action='store_true',help='Use nginx?')
        
        return parser

class WidefieldArgs(PipeArgs):

    def cmdline(self):
        parser = super(WidefieldArgs,self).argument_parser()

        # Science arguments
        parser.add_argument('--expnum',help='A single expnum or comma-separated list of expnums')
        parser.add_argument('--list',help='File of line-separated expnums')
        parser.add_argument('--exptag',help='Grab all expnums with given tag in exposuretag table')
        parser.add_argument('--epoch',help='Observing epoch. If not specified, will be calculated. E.g., \
                         SVE1,SVE2,Y1E1,Y1E2,Y2E1,Y2E2...')
        parser.add_argument('--calnite',help='bias/flat calibration nite/niterange,\
                                          i.e., 20151020 or 20151020t1030')
        parser.add_argument('--calrun',help='bias/flat calibration run, i.e., r1948p03')
        parser.add_argument('--caltag',help='Tag in OPS_PROCTAG to use if you \
                         calnite/calrun not specified')

        args = parser.parse_args()

        return args

class NitelycalArgs(PipeArgs):

    def cmdline(self):
        parser = super(NitelycalArgs,self).argument_parser()

        # Science arguments
        parser.add_argument('--biaslist',help='list of line-separated bias expnums')
        parser.add_argument('--flatlist',help='list of line-separated flat expnums')
        parser.add_argument('--minnite',type=int,help='to create a supercal or many precals specify minnite\
                         along with maxnite')
        parser.add_argument('--maxnite',type=int,help='to create a supercal or many precals specify maxnite\
                         along with minnite')
        parser.add_argument('--combine',action='store_true',help='combine all exposures found into one submit')
        parser.add_argument('--count',action='store_true',help='print number of calibrations found')

        args = parser.parse_args()

        return args

class HostnameArgs(PipeArgs):

    def cmdline(self):
        parser = super(HostnameArgs,self).argument_parser()
        args = parser.parse_args()

        return args
