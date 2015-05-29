#!/usr/bin/env python

import os, sys
from tempfile import mkstemp
from shutil import move
import shutil
from os import remove, close
from PipeBox import replace_fh,ask_string,check_file


# user configuration template
template = os.path.join(os.environ['PIPEBOX_DIR'],"supportwcl/generic_cfg.des")

f = open(template,'r')
fh = f.read()

# User name
user = ask_string('Enter Username: ', default=os.environ['USER'], check=None)
fh = replace_fh(fh,'{USER}',subst=user)
# Email
email = ask_string('Enter email: ', default='%s@illinois.edu' % user, check=None)
fh = replace_fh(fh,'{EMAIL}', subst=email)
# DES Services file
des_services_path = ask_string('Enter DESservices path: ',
                               default=os.path.join(os.environ['HOME'],".desservices.ini"),
                               check=check_file)
fh = replace_fh(fh,'{DES_SERVICES_PATH}', subst=des_services_path)
# Fermi ID (optional)
fermi_id = ask_string('Fermi ID: ', default='FERMI_ID', check=None)
fh = replace_fh(fh,'{FERMI_ID}', subst=fermi_id)


# Define PIPEBOX_WORK
try:
    pipebox_work = os.environ['PIPEBOX_WORK']
except:
    pipebox_work = os.path.join(os.environ['HOME'],'PIPEBOX_WORK')
pipebox_work_path = ask_string('Enter PIPEBOX_WORK location: ', default=pipebox_work)

# Define the output name
user_config_file = "%s/config/%s_cfg.des" % (pipebox_work_path,user)

# Make sure directory exists
dirname = os.path.dirname(user_config_file)
if not os.path.exists(dirname):
    os.makedirs(dirname)

conf = open(user_config_file,'w')
conf.write(fh)
conf.close()

print "\nConfiguration file written to:\n\t%s" % user_config_file
print "\nMake sure you setup PIPEBOX_WORK environmental variable:"
print "\texport PIPEBOX_WORK=%s" % pipebox_work_path
print "\tor"
print "\tsetenv PIPEBOX_WORK %s" % pipebox_work_path
