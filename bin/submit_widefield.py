#!/usr/bin/env python

from pipebox import pipeline

# initialize and get options
widefield = pipeline.WideField()
args = widefield.args

# create JIRA ticket per nite found (default)
if args.exptag:
    widefield.ticket(args,groupby='tag')
else:
    widefield.ticket(args)
# write submit files and submit if necessary
# columns should only be values that change per submit (groupby)
widefield.make_templates(columns=['nite','expnum','band'],groupby='expnum')
