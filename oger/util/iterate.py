#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Iteration utilities.
'''


import itertools as it


def iter_chunks(iterable, chunksize):
    '''
    Iterate over chunks of fixed size.
    '''
    base = iter(iterable)  # make sure items are consumed by islice()
    while True:
        chunk = peekaheaditer(it.islice(base, chunksize))
        try:
            next(chunk)
        except StopIteration:
            break
        else:
            yield chunk


def peekaheaditer(iterator):
    '''
    Iterator wrapper for yielding the first element twice.
    '''
    try:
        first = next(iterator)
    except StopIteration:
        return
    yield first
    yield first
    yield from iterator


class CacheOneIter:
    '''
    An iterator which provides a method for repeating the last item.
    '''
    def __init__(self, iterable):
        self._base = iter(iterable)
        self._current = None
        self._proceed = True

    def __iter__(self):
        return self

    def __next__(self):
        if self._proceed:
            self._current = next(self._base)
        self._proceed = True
        return self._current

    def repeat(self):
        '''
        In the next iteration, yield the same item again.

        If this is called before the first call to __next__,
        the first item will be None.
        '''
        self._proceed = False
