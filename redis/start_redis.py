from subprocess import call
import shlex

cmd = shlex.split('redis-server')
command = call(cmd, shell = False)
