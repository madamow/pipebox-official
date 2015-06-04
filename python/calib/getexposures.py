import shlex
from subprocess import Popen
from os import remove

def get_exposures(db_section,nite):
    check_precal_string = "/work/users/mjohns44/desdm/devel/opstoolkit/trunk/bin/check_for_precal_inputs.py --section %s --band u,g,r,i,z,Y,VR --CalReq 20,10,10,10,10,10,10,10 --night %s " % (db_section,nite)
    print check_precal_string
    log = "input_exposures.list" 
    logfile = open(log,'a')
    check_precal_command_pieces = shlex.split(check_precal_string)
    check_precal_command = Popen(check_precal_command_pieces,stdout = logfile, stderr = logfile, shell = False)
    check_precal_command.communicate()
    
    for lines in open(log).readlines():
        if 'bias_expnum' in lines:
            bias_expnums = lines
        if 'dflat_expnum' in lines:
            dflat_expnums = lines
    try:
       bias_expnums 
    except:
        bias_expnums = 'None'
        dflat_expnums = 'None'
    remove(log) 
    
    return bias_expnums,dflat_expnums
