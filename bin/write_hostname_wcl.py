#!/usr/bin/env python

import os,sys
from pipebox import pipeline

# initialize and get options
hostname = pipeline.HostName()
args  = hostname.args

# create JIRA ticket
hostname.ticket()

# write submit file
hostname.make_templates()

# submit submit file or save submit bash script
hostname.submit_or_save()
