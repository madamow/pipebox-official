#!/usr/bin/env python

from pipebox import pipebox_utils, copy_pipeline as pipeline

# initialize and get options
widefield = pipeline.WideField()
args = widefield.args

if args.auto:
    # run auto-submit 
    widefield.auto(args)
else:
    # create JIRA ticket per nite found (default)
    widefield.ticket(args)
    # write submit files and submit if necessary
    widefield.make_templates()
