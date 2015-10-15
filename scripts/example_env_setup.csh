#!/bin/tcsh 

### Setup EUPS ENV ###
unsetenv EUPS_DIR
unsetenv EUPS_PATH

source /work/apps/RHEL6/dist/eups/desdm_eups_setup.csh

#setup -v Y2Nstack 1.0.5+0
setup -v Y2Nstack 1.0.6+1
setup -v jirapython 0.16+1
setup -v despydb
setup -v despyServiceAccess

#.desservices sections
setenv DES_SERVICES   $HOME/.desservices.ini
setenv DES_DB_SECTION db-destest
