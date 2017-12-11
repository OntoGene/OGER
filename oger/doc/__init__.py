#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Internal text representation.
'''


from .tsv import TSVFormatter
from .xml import EntityXMLFormatter, TextXMLFormatter
from .bioc import BioCFormatter
from .odin import ODINFormatter
from .brat import BratFormatter
from .becalm import BeCalmTSVFormatter, BeCalmJSONFormatter


# Keep this mapping up to date.
EXPORTERS = {
    'tsv': TSVFormatter,
    'text_tsv': TSVFormatter,
    'xml': EntityXMLFormatter,
    'text_xml': TextXMLFormatter,
    'bioc': BioCFormatter,
    'odin': ODINFormatter,
    'brat': BratFormatter,
    'becalm_tsv': BeCalmTSVFormatter,
    'becalm_json': BeCalmJSONFormatter,
}

OUTFMTS = list(EXPORTERS.keys())
