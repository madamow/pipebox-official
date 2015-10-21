from jinja2 import Environment,FileSystemLoader
import os


templates_dir = os.path.join(os.environ['PIPEBOX_DIR'],'templates')
env = Environment(loader=FileSystemLoader(templates_dir),trim_blocks=True)

# for jinja2 2.7+
#env = Environment(loader=FileSystemLoader(templates_dir),trim_blocks=True,lstrip_blocks=True)

from .pipebox_utils import *
