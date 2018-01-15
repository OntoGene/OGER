#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter base classes.
'''


import os
import io

from lxml import etree


class Formatter:
    '''
    Base class for all formatters.
    '''
    ext = None
    binary = False  # text or binary format?

    def __init__(self, config, fmt_name):
        self.config = config
        self.fmt_name = fmt_name

    def export(self, content):
        '''
        Write this content to disk.
        '''
        open_params = self._get_open_params(content)
        try:
            f = open(**open_params)
        except FileNotFoundError:
            # An intermediate directory didn't exist.
            # Create it and try again.
            # (Use exist_ok because of race conditions -- another
            # worker might have created it in the meantime.)
            os.makedirs(os.path.dirname(open_params['file']), exist_ok=True)
            f = open(**open_params)
        with f:
            self.write(f, content)

    def write(self, stream, content):
        '''
        Write this content to an open file.
        '''
        raise NotImplementedError()

    def dump(self, content):
        '''
        Serialise the content to str or bytes.
        '''
        raise NotImplementedError()

    def _get_open_params(self, content):
        path = self.config.get_out_path(content.id_, content.basename,
                                        self.fmt_name, self.ext)
        if self.binary:
            return dict(file=path, mode='wb')
        else:
            return dict(file=path, mode='w', encoding='utf8')


class MemoryFormatter(Formatter):
    '''
    Abstract formatter with a primary dump method.

    Subclasses must override dump(), on which write() is based.
    '''
    def write(self, stream, content):
        stream.write(self.dump(content))


class StreamFormatter(Formatter):
    '''
    Abstract formatter with a primary write method.

    Subclasses must override write(), on which dump() is based.
    '''
    def dump(self, content):
        if self.binary:
            buffer = io.BytesIO()
        else:
            buffer = io.StringIO()
        self.write(buffer, content)
        return buffer.getvalue()


class XMLMemoryFormatter(MemoryFormatter):
    '''
    Formatter for XML-based output.

    Subclasses must define a method _dump() which returns
    an lxml.etree.Element node.
    '''
    ext = 'xml'
    binary = True

    def dump(self, content):
        node = self._dump(content)
        return self._tostring(node)

    def _dump(self, content):
        raise NotImplementedError()

    @staticmethod
    def _tostring(node, **kwargs):
        kwargs.setdefault('encoding', "UTF-8")
        kwargs.setdefault('xml_declaration', True)
        kwargs.setdefault('pretty_print', True)
        return etree.tostring(node, **kwargs)
