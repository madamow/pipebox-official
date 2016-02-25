#!/usr/bin/env python

from pipebox import pipeline


# initialize and get options
nitelycal = pipeline.NitelyCal()   
args = nitelycal.args

if args.combine:
    # create ticket based on date range
    nitelycal.ticket(args,groupby='niterange')
else:
    # create tickets based on each nite
    nitelycal.ticket(args)
# write submit files for each nite and submit if necessary
nitelycal.make_templates() 
