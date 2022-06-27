# Pipebox

PipeBox is built on top of the DESDM processing framework and serves as a templating and submission tool that makes submitting many jobs easier.

## Installation

### Clone repo
```
git clone git@github.com:DarkEnergySurvey/pipebox.git
```
### Set environment (bash)
```
cd pipebox
export PIPEBOX_DIR=`pwd` 
export PYTHONPATH=$PIPEBOX_DIR/python/pipebox:$PYTHONPATH
```
### Run set-up script and follow the prompts

```
create_user_config.py 
```
### Submitting a pipeline
```
submit_{pipeline}.py -h
```
#### Required arguments
- db_section: defined in your .desservices file: for production this will usually be db-desoper
- target_site: the compute site
- eups_stack: this refers to the software stack used: {stack name} {stack version}
- campaign: this refers to the template campaign in $PIPEBOX_DIR/templates/pipelines/{pipeline}/{campaign}
