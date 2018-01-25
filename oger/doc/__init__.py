#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Internal text representation.
'''


from .txt import TXTLoader
from .pubmed import PXMLLoader, PXMLFetcher, MedlineLoader, PMCLoader, PMCFetcher
from .tsv import TSVFormatter
from .xml import EntityXMLFormatter, TextXMLFormatter
from .bioc import BioCLoader, BioCFormatter
from .odin import ODINFormatter
from .brat import BratFormatter
from .becalm import BeCalmAbstractFetcher, BeCalmPatentFetcher
from .becalm import BeCalmTSVFormatter, BeCalmJSONFormatter
from .json import JSONFormatter


# Keep these mappings up to date.
LOADERS = {
    'txt': TXTLoader,
    'bioc': BioCLoader,
    'becalmabstracts': BeCalmAbstractFetcher,
    'becalmpatents': BeCalmPatentFetcher,
    'pubmed': PXMLFetcher,
    'pxml': PXMLLoader,
    'pxml.gz': MedlineLoader,
    'pmc': PMCFetcher,
    'nxml': PMCLoader,
}

INFMTS = list(LOADERS.keys())

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
    'json': JSONFormatter,
}

OUTFMTS = list(EXPORTERS.keys())
