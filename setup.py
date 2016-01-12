import shutil
import os
import sys
import glob

dest = sys.argv[1]
allcontents = glob.glob("*")

for f in allcontents:

    if os.path.isdir(f):
        shutil.copytree(f, os.path.join(dest,f))
    else:
        shutil.copyfile(f, os.path.join(dest,f))
    print "%s --> %s" % (f,os.path.join(dest,f))
