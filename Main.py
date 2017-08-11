#
##  MCRP Server W/O Security
#
#   Author: Ranjeet Kumar
#   Start Date: 16/05/2017
#   Revised Start Date: 11-Aug-2017
#   Git Path: https://github.com/xs2ranjeet/mcrp
#

import os
import sys
import time
import zmq
from logconfig import setup_logging
import logging
from optparse import OptionParser
from ServerConn import StartServer



if __name__ == '__main__':
    if zmq.zmq_version_info() < (4,0):
        raise RuntimeError("Security is not supported in libzmq version < 4.0. libzmq version {0}".format(zmq.zmq_version()))
    parser = OptionParser()
    parser.add_option('-l', '--log-level', dest='log_level', default='info', help='log level.  Valid values are "debug", "info", "warning", "error", "critical", in decreasing order of verbosity. Defaults to "info" if parameter not specified.')
    parser.add_option('-f', dest='logfile', help='If present, a logfile will be used.') 
    (options,args) = parser.parse_args()
    
    log_level = getattr(logging, options.log_level.upper(), 'INFO')    
    setup_logging(default_path=options.logfile, default_level=log_level)    
    
    logger = logging.getLogger()
    StartServer()
    

