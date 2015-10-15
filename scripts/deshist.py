#! /usr/bin/env python

import os
import sys
import time

date = sys.argv[1] # e.g., '03/13/2015'
section = sys.argv[2]

command = """deshist --cols=run,status,operator,blkcnt,lastblk,lastmod-l,lastmod-h,target_site,pipever,starttime,wallclock,endtime --section=%s --operator mjohns44 --submitdate %s""" % (section,date)

os.system(command)
