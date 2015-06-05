import os
import sys
import time
from despydb import desdbi

def replace_file(file_path, pattern, subst='', prompt=''):
    """ Replace in place for file"""
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


def replace_fh(fh,pattern,subst='',prompt=''):
    """ Replace in place for file-handle"""
    if prompt != '':
        subst = raw_input(prompt+' ')
    fh = fh.replace(pattern,subst)
    return fh


def get_expnum_info(expnum,db_section='db-destest'):

    dbh = desdbi.DesDbi(section=db_section)
    cur = dbh.cursor()
    QUERY = '''SELECT nite,band from exposure where expnum={expnum}'''
    cur.execute(QUERY.format(expnum=expnum))
    nite, band = cur.fetchone()
    return nite, band


# --------------------------------------------------------------
# These functions were copied/adapted from desdm_eupsinstal.py

def flush(f):
    f.flush();
    time.sleep(0.05)

def check_file(filename):
    if os.path.exists(filename):
        return
    else:
        return " File not found "

def ask_string(question, default, check=None):
   
    ask_again = True
    answer = None
    while(ask_again):
        ask_again = False
        sys.stdout.write("\n" + question + "\n")
        sys.stdout.write("[%s] : " % default)
        flush(sys.stdout)
        line = sys.stdin.readline()
        if line:
            line = line.strip()
            answer = None
            if not line:
                answer = default
            else:
                answer = line
            
            if check != None:
                message = check(answer)
                if message:
                    sys.stdout.write("\n")
                    flush(sys.stdout)
                    sys.stderr.write(message + "\n")
                    flush(sys.stderr)
                    ask_again = True
        else:
            sys.stdout.write("\n")
            flush(sys.stdout)
            sys.stderr.write("Reached end of input. Aborting.\n")
            sys.exit(2)
    return answer

def ask_bool(question, default):
    ask_again = True
    while ask_again:
        ask_again = False
        sys.stdout.write("\n" + question + "\n")
        if default:
            sys.stdout.write("[yes] : ")
        else:
            sys.stdout.write("[no] : ")
        flush(sys.stdout)
        line = sys.stdin.readline()
        if line:
            line = line.strip().lower()
            if not line:
                answer = default
            else:
                if line == "y" or line == "yes":
                    answer = True
                elif line == "n" or line == "no":
                    answer = False
                else:
                    answer = None
                    ask_again = True
                    sys.stdout.write("\n")
                    flush(sys.stdout)
                    sys.stderr.write("Please answer with 'yes' or 'no'.\n")
                    flush(sys.stderr)
            if answer:
                return answer

# --------------------------------------------------------------
