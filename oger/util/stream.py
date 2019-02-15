#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Streaming utilities.
'''


import io
import os
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


def text_stream(source, encoding='utf-8', **kwargs):
    '''
    If needed, open and decode a text stream from a path, URL, or open file.
    '''
    # Source is a stream.
    if hasattr(source, 'read'):
        # Check if this stream needs decoding.
        if isinstance(source, (io.RawIOBase, io.BufferedIOBase)):
            source = codecs.getreader(encoding)(source)
        return source
    # Source is a path/URL.
    return ropen(source, encoding=encoding, **kwargs)


def basename(source):
    '''
    Try to get a base filename.
    '''
    if hasattr(source, 'name'):
        source = source.name
    if isinstance(source, str):
        return os.path.basename(source)
    return None
