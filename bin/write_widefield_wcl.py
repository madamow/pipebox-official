#!/usr/bin/env python

from pipebox import pipeline

# initialize and get options
widefield = pipeline.WideField()
args = widefield.args

if args.auto:
    # run auto-submit 
    widefield.auto(args)
else:
    # create JIRA ticket per nite found (default)
    if args.exptag:
        widefield.ticket(args,groupby='tag')
    else:
        widefield.ticket(args)
    # write submit files and submit if necessary
    widefield.make_templates()
