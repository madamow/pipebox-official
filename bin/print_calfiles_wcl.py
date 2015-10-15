#!/usr/bin/env python

from se_calib import calib_info as cals
from datetime import datetime
import os,sys

def cmdline():
    import argparse
    parser = argparse.ArgumentParser(description="Find calibration files closest to a given date")
    # The positional arguments
    parser.add_argument("nite", action="store",
                        help="Night that we want to move (i.e. 20121123)")
    # Optional arguments
    parser.add_argument("--archive_name", default="prodbeta",   
                        help="Archive name (i.e. prodbeta or desar2home)")
    parser.add_argument("--db_section",default="db-destest",action="store",
                        choices=['db-desoper','db-destest'], help="Database section")
    return parser.parse_args()

if __name__ == "__main__":

    # Get the options
    args  = cmdline()
    info = cals.get_cals_info(**args.__dict__)
    cals.print_wcl_calconfig(info,args.nite)

    #blck = cals.construct_wcl_block(info,args.nite)
    #print blck

