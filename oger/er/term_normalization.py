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

from ..util.stream import text_stream

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
    from functools import lru_cache
    from nltk.stem import LancasterStemmer, PorterStemmer
    options = {'lancaster': LancasterStemmer, 'porter': PorterStemmer}
    stemmer = options[which.lower()]
    cached_callable = lru_cache(2**16)(stemmer().stem)
    return cached_callable


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


def _load_mask(repl='0', target='digits', *args):
    '''
    Mask certain tokens through a replacement.

    The target parameter can be a special name like "digits"
    or a path to a file with a list of targets (one token
    per line).
    '''
    if args:
        # Try to recover from filenames containing dashes,
        # which were interpreted by the load function as
        # argument separators.
        target = '-'.join((target, *args))

    if target == 'digits':
        test = str.isdigit
    elif target == 'numeric':
        test = str.isnumeric
    elif target == 'punct':
        test = re.compile(r'[^\w\s]+').fullmatch
    else:
        # Read targets from a file.
        with text_stream(target) as f:
            target_tokens = frozenset(t.strip() for t in f)
        test = target_tokens.__contains__

    def mask(token):
        '''Mask certain tokens.'''
        if test(token):
            return repl
        return token

    return mask
