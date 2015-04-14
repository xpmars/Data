#!/usr/bin/env python

import os
from logbook import FileHandler
from clean.periodic_clean import PeriodicClean


if __name__ == "__main__":
    current_path = os.path.split(os.path.realpath(__file__))[0]
    # initialize log system
    file_handler = FileHandler(current_path + "/hive_clean.log", bubble=True)
    file_handler.push_application()
    # execute clean task
    PeriodicClean.run()
