#!/usr/bin/env python

import os,sys
from pipebox import write_template, ALL_CCDS

def cmdline():

    import argparse
    parser = argparse.ArgumentParser(description="Creates the submit for hostname testing\nwrite_firstcut_wcl_hostname.py  1124")
    # The positional arguments
    parser.add_argument("reqnum", action="store",default=None,
                        help="Request number")
    parser.add_argument("--db_section", action="store", default='db-destest',
                        choices=['db-desoper','db-destest'],
                        help="DB Section to query")
    parser.add_argument("--archive_name", default=None,   
                        help="Archive name (i.e. prodbeta or desar2home)")
    parser.add_argument("--schema", default=None,   
                        help="Schema name (i.e. prodbeta or prod)")
    parser.add_argument("--http_section", default=None,   
                        help="DES Services http-section  (i.e. file-http-prodbeta)")
    parser.add_argument("--target_site", action="store", default='fermigrid-sl6',
                        help="Compute Target Site")
    parser.add_argument("--user", action="store", default=os.environ['USER'],
                        help="username that will submit")
    parser.add_argument("--labels", action="store", default='me-tests',
                        help="Coma-separated labels")
    parser.add_argument("--eups_product", action="store", default='firstcut',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--eups_version", action="store", default='Y3Ndev+1',
                        help="Name of the EUPS stack to use")
    parser.add_argument("--campaign", action="store", default='Y3T',
                        help="Name of the campaign")
    parser.add_argument("--project", action="store", default='ACT',
                        help="Name of the project ie. ACT/FM/etc")
    parser.add_argument("--libname", action="store", default='Y3Ndev',
                        help="Name of the wcl library to use")
    parser.add_argument("--template", action="store", default='hostname',
                        help="Name of template to use (without the .des)")
    parser.add_argument("--verb", action="store_true", default=False,
                        help="Turn on verbose")
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
        
    submit_template_path = os.path.join("pipelines/hostname","hostname_template.des")
    output_name = "%s_hostname_rendered_template.des" %s (args.reqnum)
    output_path = os.path.join(args.pipebox_work,output_name)
    wclname = write_template(submit_template_path,output_path,args)
   
    if args.savefiles: 
        # Current schema of writing dessubmit bash script
        bash_script_name = "submitme_hostname_%s_%s.sh" % (args.reqnum,args.target_site)
        bash_script_path= os.path.join(args.pipebox_work,bash_script_name)
        args.rendered_template_path.append(output_path)
        os.chmod(bash_script_path, 0755)
        print "# To submit files:\n"
        print "\t %s\n " % bash_script_name

        # Print warning of Fermigrid credentials
        if args.target_site == 'fermigrid-sl6':
            print "# For FermiGrid please make sure your credentials are valid"
            print "\t setenv X509_USER_PROXY $HOME/.globus/osg/user.proxy"
            print "\t voms-proxy-info --all"
    else:
        # Placeholder for submitting jobs
        pipebox_utils.submit_exposure(output_name)
