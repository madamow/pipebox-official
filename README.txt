### Work in progress ###

# Add Pipebox dir to your environment using eups
1. in this directory type: setup -j -r .

# Create user-specific configuration file 
2. Run ./bin/create_user_cfg.py # to create user specific configuration file

# Create submitwcl for particular pipeline from template
3. e.g., Run ./bin/write_sne_wcl_exposure.py # -h for command-lines

# Run submit script to process data as specified in submitwcl
4. Run $PIPEBOX_WORK/submitme*.sh

# Wait for pass or fail email!
