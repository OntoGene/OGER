#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Streaming utilities.
'''


import codecs
import urllib.request


REMOTE_PROTOCOLS = ('http://', 'https://', 'ftp://')


def ropen(locator, encoding='utf-8', **kwargs):
    '''
    Open a local or remote file for reading.
    '''
    if locator.startswith(REMOTE_PROTOCOLS):
        r = urllib.request.urlopen(locator)
        f = codecs.getreader(encoding)(r)
    else:
        f = open(locator, encoding=encoding, **kwargs)
    return f
