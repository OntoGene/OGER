#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Miscellaneous utilities.
'''


import codecs
import logging


class BackwardsCompatibility:
    '''
    Check changed parameters and report obsolete use.
    '''
    def __init__(self, **renamed):
        self.renamed = renamed
        self._warnings = []

    def items(self, params):
        '''
        Iterate over up-to-date key-value pairs.
        '''
        for key, value in params.items():
            if key in self.renamed:
                self._warnings.append((key, self.renamed[key]))
                key = self.renamed[key]
            yield key, value

    def warnings(self):
        '''
        Issue warnings for the use of obsolete parameters.
        '''
        for obs, new in self._warnings:
            logging.warning(
                'parameter %s is obsolete, use %s instead', obs, new)


def codepoint_indices(text, codec):
    '''
    Create a list of byte offsets for each character.

    The returned list has a length of `len(text)+1`, as it
    includes the end offset as well.
    '''
    # Use UTF-8 optimisation if applicable.
    if codecs.lookup(codec).name == 'utf-8':  # lookup: get canonical spelling
        indices = iter_codepoint_indices_utf8(text)
    else:
        indices = iter_codepoint_indices(text, codec)
    return list(indices)

def iter_codepoint_indices(text, codec):
    '''
    Iterate over the bytes offset of each character (any codec).
    '''
    # Note: for encodings with a BOM, the first offset probably shouldn't
    # be 0, but 2, 3, or 4, depending on the BOM's length.
    # This is ignored due to the lack of expected practical applications.
    i = 0
    for b in codecs.iterencode(text, codec):
        yield i
        i += len(b)
    yield i

def iter_codepoint_indices_utf8(text):
    '''
    Iterate over the bytes offset of each character (UTF-8 only).
    '''
    octets = text.encode('utf-8')
    for i, c in enumerate(octets):
        # Enumerate all bytes, but omit the continuation bytes (10xxxxxx).
        if not 0x80 <= c < 0xC0:
            yield i
    yield len(octets)


def get_offset_manager(anchor):
    '''
    Get an actual or dummy offset manager.
    '''
    if not anchor:
        return BioCOffsetManager()
    else:
        return BioCByteOffsetManager(anchor)

class BioCOffsetManager:
    '''
    Offer the same interface as BioCByteOffsetManager
    without doing anything interesting.
    '''

    @staticmethod
    def passage(section):
        '''
        Return the start offset of this passage/section.
        '''
        return section.start

    @staticmethod
    def sentence(sentence):
        '''
        Return the start offset of this sentence.
        '''
        return sentence.start

    @staticmethod
    def entity(entity):
        '''
        Return start and length of this annotation.
        '''
        return entity.start, entity.end-entity.start

class BioCByteOffsetManager(BioCOffsetManager):
    '''
    Keep track of bytes offsets.
    '''
    def __init__(self, anchor):
        self.anchor = anchor
        self._cumulated = 0
        self._last_sentence = 0
        self._sent_start = None
        self._cp_index = None

    def passage(self, section):
        '''
        New passage/section.

        Update counts and return the cumulated start offset.
        '''
        if self.anchor == 'passage':
            self._cumulated = section.start
        else:
            self._cumulated += self._last_sentence
            self._last_sentence = 0
        return self._cumulated

    def sentence(self, sentence):
        '''
        New sentence.

        Update counts and return the cumulated start offset.
        '''
        self._cp_index = list(iter_codepoint_indices_utf8(sentence.text))
        self._sent_start = sentence.start
        if self.anchor == 'sentence':
            self._cumulated = sentence.start
        else:
            self._cumulated += self._last_sentence
            self._last_sentence = self._cp_index[-1]
        return self._cumulated

    def entity(self, entity):
        '''
        Calculate start and length for this annotation.
        '''
        start, end = (self._cp_index[n-self._sent_start]+self._cumulated
                      for n in (entity.start, entity.end))
        return start, end-start
