import os
from jinja2 import Environment,FileSystemLoader
#from pipebox import pipeline

#PIPELINE = pipeline.PipeLine()
#PIPELINE.templates_dir = os.path.join(PIPELINE.pipebox_dir,'templates')
#PIPELINE.env = Environment(loader=FileSystemLoader(PIPELINE.templates_dir),trim_blocks=True)

templates_dir = os.path.join(os.environ['PIPEBOX_DIR'],'templates')
env = Environment(loader=FileSystemLoader(templates_dir),trim_blocks=True)

# for jinja2 2.7+
#env = Environment(loader=FileSystemLoader(templates_dir),trim_blocks=True,lstrip_blocks=True)

from .pipebox_utils import *
