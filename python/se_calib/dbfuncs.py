
''' DB function helpers'''

def get_dbh(db_section='desoper',verb=False):
    """ Get a DB handle"""
    from despydb import desdbi
    if verb: print "# Creating db-handle to section: %s" % db_section
    dbh = desdbi.DesDbi(section=db_section)
    return dbh

def get_root_archive(dbh, archive_name='prodbeta',verb=False):
    """ Get the root-archive fron the database"""
    cur = dbh.cursor()
    # Get root_archive
    query = "select root from ops_archive where name='%s'" % archive_name
    if verb:
        print "# Getting the archive root name for section: %s" % archive_name
        print "# Will execute the SQL query:\n********\n** %s\n********" % query
    cur.execute(query)
    root_archive = cur.fetchone()[0]
    if verb: print "# root_archive: %s" % root_archive
    return root_archive
