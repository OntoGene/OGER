#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2018


'''
OGER postfilter for removing frequent FPs.
'''


import re

from oger.doc import document


def frequentFP(content):
    '''
    Remove all entities that match the pattern.
    '''
    for sentence in content.get_subelements(document.Sentence):
        for entity in list(sentence.entities):
            if is_bad(entity.text):
                sentence.entities.remove(entity)


def is_bad(span):
    '''
    Detect some frequent bad pattern.
    '''
    if bad_terms.fullmatch(span):
        return True
    if span.lower() in misc_bad:
        return True
    if '<' in span or '=' in span or '>' in span:
        return True
    return False


stopwords = (
    # General language.
    'a all and as at be for in is of on or per the to was '
    # Units.
    'cm kg ml mm mol μg μl μm μs '
    # Miscellaneous (eg. "P < .001").
    'ci d n hr max min p ph pi sp '
).split()
stopwords += [w.title() for w in stopwords if len(w) > 1]
stopwords = map(re.escape, stopwords)
stopword_number = r'(?:{})\W+\d+'.format('|'.join(stopwords))

micro_unit = r'μ[A-Za-z][\W\d]*'

bad_terms = re.compile('|'.join((stopword_number, micro_unit)))


misc_bad = '''\
to a
a 3d
'''

misc_bad = frozenset(misc_bad.split('\n'))
