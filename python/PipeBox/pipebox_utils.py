
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
