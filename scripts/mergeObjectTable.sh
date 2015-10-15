#!/bin/sh

# Want to minimize partitions, run once per "nite" after exposures for nite are done

nite=20121124 # for human-readability
reqnum=1413
pipeline=finalcut

main_obj_table=PRODBETA.SE_OBJECT

# For "normal" ingest:
cmd="merge_objects.py -targettable=${main_obj_table} -request=${reqnum}"
echo $cmd
$cmd
