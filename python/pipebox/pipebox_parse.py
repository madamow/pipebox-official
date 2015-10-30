"""
A Collection of utilities to connect configuration files with argparse and ConfParser
"""

from ConfigParser import SafeConfigParser, NoOptionError
import ConfigParser
import argparse

def build_conf_parser():

    """ Create a paser with argparse and ConfigParser to load default arguments """

    # Parse any conf_file specification
    # We make this parser with add_help=False so that
    # it doesn't parse -h and print help.
    conf_parser = argparse.ArgumentParser(
        description=__doc__, # printed with -h/--help
        # Don't mess with format of description
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # Turn off help, so we print all options in response to -h
        add_help=False
        )
    conf_parser.optionxform = str
    conf_parser.add_argument("-c", "--conf_file",
                             help="Specify config file")
    args, remaining_argv = conf_parser.parse_known_args()
    if args.conf_file:
        if not os.path.exists(args.conf_file):
            print "# WARNING: configuration file %s not found" % args.conf_file
        config = ConfigParser.RawConfigParser()
        config.optionxform=str # Case sensitive
        config.read([args.conf_file]) # Fix True/False to boolean values
        updateBool(config) # Fix bool
        updateNone(config) # Fix None
        defaults = {}
        for section in config.sections():
            defaults.update(dict(config.items(section)))
        return conf_parser,defaults

def updateList(config,option):
    for section in config.sections():
        for opt,val in config.items(section):
            if opt == option: config.set(section,opt,val.split(','))
    return config

def updateBool(config):
    # Reading all sections and dump them in defaults dictionary
    for section in config.sections():
        for option,value in config.items(section):
            if value == 'False' or value == 'True':
                bool_value = config.getboolean(section, option)   
                print "# Updating %s: %s --> bool(%s) section: %s" % (option,value,bool_value, section)
                config.set(section,option, bool_value)
    return config

def updateNone(config):
    # Reading all sections and dump them in defaults dictionary
    for section in config.sections():
        for option,value in config.items(section):
            if value == 'None' or value == 'none':
                config.set(section,option,None)
    return config
