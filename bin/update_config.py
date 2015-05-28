import os, sys
from tempfile import mkstemp
from shutil import move
import shutil
from os import remove, close

def replace(file_path, pattern, subst='', prompt=''):
    if prompt != '':
        subst = raw_input(prompt+' ')
    fh, abs_path = mkstemp()
    with open(abs_path,'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    close(fh)
    remove(file_path)
    move(abs_path, file_path)

#os.chdir('supportwcl')

template=os.path.join(os.path.join(os.getcwd(),'supportwcl'),'generic_cfg.des')

user=raw_input('Enter username : ')
file_user=os.path.join(os.path.join(os.getcwd(),'supportwcl'),user+'_cfg.des')
os.system('cp '+template+' '+file_user)

replace(file_user,'{USER}',subst=user)
replace(file_user,'{EMAIL}', prompt='Enter email :')
replace(file_user,'{PROJECT}', prompt='Enter project :')
replace(file_user,'{DES_SERVICES_PATH}', prompt='Enter desservices path :')
replace(file_user,'{DES_DB_SECTION}', prompt='Enter DB section :')
replace(file_user,'{HTTP_SECTION}', prompt='Enter HTTP section :')
replace(file_user,'{HOME_ARCHIVE}', prompt='Enter Home archive :')



