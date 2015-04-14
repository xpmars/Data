# -*- coding: utf-8 -*-

from logbook import Logger


class BaseObject(object):

    def __init__(self):
        self.logger = Logger(type(self).__name__)

if __name__ == "__main__":
    obj = BaseObject()
