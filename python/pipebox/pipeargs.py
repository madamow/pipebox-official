import os
from configargparse import ArgParser
from pipebox import jira_utils

class PipeArgs(object):

    @staticmethod 
    def argument_parser():
        # Create command line arguments
        parser = ArgParser()
        # General arguments
        parser.add('--db_section',required=True,help = "Database section in your \
                             .desservices.ini file, e.g., db-desoper or db-destest")
        parser.add("--user", action="store", default=os.environ['USER'],
                            help="username that will submit")
        parser.add('--paramfile',is_config_file=True,help='Key = Value file that can be used to replace\
                             command-line')
        parser.add('--csv',help='CSV of exposures and information specified by user. If specified, \
                             code will use exposures in csv to submit jobs. Must also specify \
                             --delimiter')
        parser.add('--delimiter',default=',',help='The delimiter if specifying csv and is not \
                             comma-separated')
        parser.add('--campaignlib',required=True, help='Directory in pipebox where templates are \
                             stored, e.g., $PIPEBOX_DIR/templates/pipelines/finalcut/-->Y2A1dev<--')
        parser.add('--savefiles',action='store_true',help='Saves submit files to submit later.')
        parser.add('--queue_size',default=1000,help='If set and savefiles is not specified, code \
                             will submit specified runs up until queue_size is reached. Code \
                             will wait until queue drops below limit to submit next job')
        parser.add('--total_queue',action='store_true',help='If specified, total jobs per \
                             pipeline per machine will be counted and user will be ignored')
        parser.add('--labels',help='Human-readable labels to "mark" a given processing attempt')
        parser.add('--template_name',help='submitwcl template within pipeline/campaign')
        parser.add('--configfile',help='Name of user cfg file')
        parser.add('--out',help='Output directory for submit files')
        parser.add('--auto',action='store_true',help='Will run autosubmit mode if specified')
        parser.add('--resubmit_failed',action='store_true',help='Will ressubmit failed runs')
        parser.add('--ignore_processed',action='store_true',help='Will skip any expnum \
                             that has been attempted to process, pass/fail.')
        parser.add('--wait',default=30,help='Wait time (seconds) between dessubmits. \
                                             Default=30s')
        
        # Archive arguments
        parser.add('--target_site',required=True,help='Computing node, i.e., fermigrid-sl6')
        parser.add('--http_section',help='')
        parser.add('--archive_name',help='Home archive to store products, e.g., \
                             desar2home,prodbeta,...')
        parser.add('--campaign',required=True,help='Used in archive dir, e.g., Y2T3')
        parser.add('--project',default='ACT',help='Archive directory where runs are \
                             stored, e.g., $ARCHIVE/-->ACT<--/finalcut/')
        parser.add('--rundir',help='Archive directory structure')
      
        # JIRA arguments
        parser.add('--jira_parent',help='JIRA parent ticket under which\
                             new ticket will be created.')
        parser.add('--jira_description',help='Description of ticket\
                             found in JIRA')
        parser.add('--jira_project',default='DESOPS',help='JIRA project where \
                             ticket will be created, e.g., DESOPS')
        parser.add('--jira_summary',help='Title of JIRA ticket. To submit multiple \
                             exposures under same ticket you can specify jira_summary')
        parser.add('--jira_user',default = jira_utils.get_jira_user(),help='JIRA username')
        parser.add('--jira_section',default='jira-desdm',help='JIRA section \
                             in .desservices.ini file')
        parser.add('--ignore_jira',default=False,action='store_true',help="If specified will not \
                            connect to JIRA, but must specify reqnum and jira_parent.")
        parser.add('--reqnum',help='Part of processing unique identifier. Tied to JIRA ticket \
                             number')
    
        # EUPS arguments
        parser.add('--eups_product',required=True,help='EUPS production stack, e.g., finalcut')
        parser.add('--eups_version',required=True,help='EUPS production stack version, e.g., Y2A1+1')
        
        # Science arguments
        parser.add('--ccdnum',help='CCDs to be processed.')
        parser.add('--nite',help='For auto mode: if specified will submit all exposures found \
                         from nite')
        parser.add('--epoch',help='Observing epoch. If not specified, will be calculated. E.g.,\
                         SVE1,SVE2,Y1E1,Y1E2,Y2E1,Y2E2...')

        # glide in options
        parser.add('--time_to_live',default=None,type=float,help='The amount of time-to-live (in hours)\
                          for the job to grab a glidein')
        
        # Transfers
        parser.add('--nginx',action='store_true',help='Use nginx?')
        
        return parser

class SupernovaArgs(PipeArgs):

    def cmdline(self):
        parser = super(SupernovaArgs,self).argument_parser()

        # Science arguments
        parser.add('--triplet',help='A single triplet formated as "nite,field,filter" (e.g. "20160114,C3,g")')
        parser.add('--list',help='File of line-separated triplets (no commas)')
        # Reasonable certain we don't want this
        # parser.add('--exptag',help='Grab all expnums with given tag in exposuretag table')
        parser.add('--calnite',help='bias/flat calibration nite/niterange,\
                                          i.e., 20151020 or 20151020t1030')
        parser.add('--calrun',help='bias/flat calibration run, i.e., r1948p03')
        parser.add('--caltag',help='Tag in OPS_PROCTAG to use if you \
                         calnite/calrun not specified')

        args = parser.parse_args()

        return args

class WidefieldArgs(PipeArgs):

    def cmdline(self):
        parser = super(WidefieldArgs,self).argument_parser()
        
        parser.add('--after_merge',action='store_true',help='Run in mode to directly insert\
                            objects into SE_OBJECT table')
        # Science arguments
        parser.add('--expnum',help='A single expnum or comma-separated list of expnums')
        parser.add('--list',help='File of line-separated expnums')
        parser.add('--exptag',help='Grab all expnums with given tag in exposuretag table')
        parser.add('--calnite',help='bias/flat calibration nite/niterange,\
                                          i.e., 20151020 or 20151020t1030')
        parser.add('--calrun',help='bias/flat calibration run, i.e., r1948p03')
        parser.add('--caltag',help='Tag in OPS_PROCTAG to use if you \
                         calnite/calrun not specified')
       
        # Exposure query for args.nite
        parser.add('--propid',default=['2012B-0001'],help="Propid in exposure table to filter expnums \
                        on by nite")
        parser.add('--program',default=['supernova','survey','photom-std-field'],help="Programs in \
                        exposure table to filter expnums on by nite")
        parser.add('--ignore_all',action='store_true',default=True,help="By default process all \
                        exposures from a given nite")
        parser.add('--ignore_program',action='store_true',default=False,help="By default process all \
                        exposures from a given nite.")
        parser.add('--ignore_propid',action='store_true',default=False,help="By default process all \
                        exposures from a given nite")
	parser.add('--RA', '-ra', nargs=2, help='RA in degrees, in the order of min, max')
	parser.add('--Dec', '-dec', nargs=2, help='Dec in degrees')
	args = parser.parse_args()

        return args

class NitelycalArgs(PipeArgs):

    def cmdline(self):
        parser = super(NitelycalArgs,self).argument_parser()

        # Science arguments
        parser.add('--biaslist',help='list of line-separated bias expnums')
        parser.add('--flatlist',help='list of line-separated flat expnums')
        parser.add('--minnite',type=int,help='to create a supercal or many precals specify minnite\
                         along with maxnite')
        parser.add('--maxnite',type=int,help='to create a supercal or many precals specify maxnite\
                         along with minnite')
        parser.add('--combine',action='store_true',help='combine all exposures found into one submit')
        parser.add('--count',action='store_true',help='print number of calibrations found')

        args = parser.parse_args()

        return args

class HostnameArgs(PipeArgs):

    def cmdline(self):
        parser = super(HostnameArgs,self).argument_parser()
        args = parser.parse_args()

        return args
