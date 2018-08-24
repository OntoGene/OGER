#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Tools for term normalization.

This module provides a single public  function `load()`
which returns one or more normalization functions.
'''


import re
import unicodedata

# Note: more imports in some of the _load_* functions.
# This avoids unnecessary imports that take a long time to load.


def load(names):
    '''
    Initialise and return one or many normalization functions.

    If names is a string, a single function is returned.
    Otherwise it should be a sequence of strings, in which
    case a tuple of functions is returned.
    '''
    if isinstance(names, str):
        return _load(names)
    else:
        return tuple(_load(name) for name in names)


def _load(expr):
    name, *args = expr.split('-')
    name = '_load_' + name
    try:
        loader = globals()[name]
        function = loader(*args)
    except Exception as e:
        raise ImportError('Cannot load normalization method {}: {!r}'
                          .format(expr, e))
    else:
        return function


def _load_lowercase():
    return str.lower


def _load_unicode(form='NFKC'):
    '''
    Perform Unicode normalization.
    '''
    # Do a quick test to make sure `form` is valid.
    unicodedata.normalize(form, 'a')
    def unicode_normalize(token):
        return unicodedata.normalize(form, token)
    unicode_normalize.__doc__ = 'Convert to canonical form {}.'.format(form)
    return unicode_normalize


def _load_stem(which='lancaster'):
    from nltk.stem import LancasterStemmer, PorterStemmer
    options = {'lancaster': LancasterStemmer, 'porter': PorterStemmer}
    stemmer = options[which.lower()]
    return stemmer().stem


def _load_greektranslit():
    '''
    Transliterate Greek letters into their spelled-out names.
    '''
    trans_map = {
        'α': 'alpha',
        'β': 'beta',
        'γ': 'gamma',
        'δ': 'delta',
        'ε': 'epsilon',
        'ζ': 'zeta',
        'η': 'eta',
        'θ': 'theta',
        'ι': 'iota',
        'κ': 'kappa',
        'λ': 'lamda',
        'μ': 'mu',
        'ν': 'nu',
        'ξ': 'xi',
        'ο': 'omicron',
        'π': 'pi',
        'ρ': 'rho',
        'ς': 'sigma',
        'σ': 'sigma',
        'τ': 'tau',
        'υ': 'upsilon',
        'φ': 'phi',
        'χ': 'chi',
        'ψ': 'psi',
        'ω': 'omega',
    }
    letters = re.compile('[{}]'.format(''.join(trans_map)))

    def _replace(match):
        return trans_map[match.group()]

    def translit(token):
        '''
        Transliterate Greek letters à la 'α' -> 'alpha'.
        '''
        return letters.sub(_replace, token)

    return translit
