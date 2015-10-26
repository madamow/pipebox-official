## Pipebox

# Installation

- setup -v -r pipebox 
- mkdir -p $HOME/PIPEBOX_WORK  (optional)
- setenv PIPEBOX_WORK $HOME/PIPEBOX_WORK  (tcsh)
- export PIPEBOX_WORK=$HOME/PIPEBOX_WORK (bash)
- create_user_config.py 
- write_firstcut_wcl_exposure.py <exp>  <req> --target_site <target> --user <user>
eg. write_firstcut_wcl_exposure.py 229686 1434 --target_site descmp4 --user mcarras2 
- source $PIPEBOX_DIR/scripts/example_env_setup.sh (or .csh)


Below is copy from Michael's Readme and added more

1. scripts
    a. test_env_setup.sh
        - sourcing this file sets up environment for processing
        - includes two environment values that need to be replaced
            i. $DES_DB_SECTION
            ii. $DES_SERVICES_PATH
2. supportwcl
    a. mypfwcfg.des is a config file that includes framework parameters and user-specific values that need to be replaced.
        - $DES_SERVICES_PATH
        - $DES_DB_SECTION
        - $USER
        - $EMAIL
        - $PROJECT #typically your initials (the dir where archived files stored)
3. submitwcl
    a. includes submit files for each pipeline (this is where most of your parameters are specified for any given "run")
        - e.g.,
        - target_site
        - reqnum
        - label
        - eups stack name/version
        - input cals
        - module order
        - etc.
    b. command: for a single sumbit use - dessubmit <submit file>
                for a mass submit using a list use submitmass2var.sh in /scripts
4. modulewcl
    a. includes the module definitions for the science codes (files used/generated, commandlines, etc.)
    b. these are included in the submit files
5. Lists
    a. includes sample exposures lists to use. You could put/name these whatever/wherever you'd like.
