#!/bin/bash

### Setup EUPS ENV ###
unset EUPS_DIR
unset EUPS_PATH

source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh

setup Y2Nstack 1.0.6+1
setup jirapython 0.16+1
setup despydb
setup despyServiceAccess

#.desservices sections
export DES_SERVICES=$DES_SERVICES_PATH
export DES_DB_SECTION=$DES_DB_SECTION
