# -----------------------------------------------------------------------------
# Name:        __init__
# Purpose:     To define the API interface for workflow package
# Authors:     aric.sanders@nist.gov,
# Created:     9/18/2020
# License:     MIT License
# -----------------------------------------------------------------------------

"""
workflow is a package for scientific workflow construction.
To change the imported API, change the dictionary API_MODULES to have an entry
DE_API_MODULE["code.subpackage.module"]=True
 in this __init__.py file.

"""

#----------------------------------------------------------------------
# Standard Imports
import os
import sys

# -----------------------------------------------------------------------------
# Third Party Imports

# -----------------------------------------------------------------------------
# Module Constants
VERBOSE_IMPORT = False
TIMED_IMPORT = False
__version__ = '0.1.7'
"Constant that determines if import statements are echoed to output"
# The new module load scheme can be for module in DE_API_MODULES.keys()
# -----------------------------------------------------------------------------
DE_API_MODULES = {"asmg_workflow.logs": True,
                  "asmg_workflow.tasks": True,
                  "asmg_workflow.workflows": True,
                  }

"Dictionary that controls the definition of the API, this can be set to leave out any unwanted modules. Also it is" \
    "possible to discover all modules by DE_API_MODULES.keys()"

# This makes sure this file is the one loaded
sys.path.append(os.path.dirname(__file__))
# To tune the imported API change the DE_API_MODULES dictionary
if TIMED_IMPORT:
    import datetime

    first_timer = datetime.datetime.utcnow()
    start_timer = datetime.datetime.utcnow()
for module in sorted(DE_API_MODULES.keys()):
    try:
        if DE_API_MODULES[module]:
            if VERBOSE_IMPORT:
                print(("Importing {0}".format(module)))
            exec('from {0} import *'.format(module))
            if TIMED_IMPORT:
                end_timer = datetime.datetime.utcnow()
                time_difference = end_timer - start_timer
                print(("It took {0} s to import {1}".format(time_difference.total_seconds(), module)))
                start_timer = end_timer
    except Exception as e:
        print(f"The {module} failed to import")
        print(e)
if TIMED_IMPORT:
    end_timer = datetime.datetime.utcnow()
    time_difference = end_timer - first_timer
    print(("It took {0} s to import all of the active modules".format(time_difference.total_seconds())))
