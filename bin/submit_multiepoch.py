#!/usr/bin/env python

from pipebox import pipeline

# initialize and get options
multiepoch = pipeline.MultiEpoch()
args = multiepoch.args

# create JIRA ticket per nite found (default)
if args.exptag:
    multiepoch.ticket(args,groupby='tag')
else:
    if not args.jira_summary:
        args.jira_summary = '_'.join([args.user,args.campaign,
                        args.desstat_pipeline,str(args.submit_time)[:10]])
    multiepoch.ticket(args,groupby='user')

# write submit files and submit if necessary
# columns should only be values that change per submit (groupby)
multiepoch.make_templates(columns=['tile'],groupby='tile')
