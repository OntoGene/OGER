#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Internal text representation.
'''


from .txt import TXTLoader, TXTJSONLoader
from .pubmed import PXMLLoader, PXMLFetcher, MedlineLoader, PMCLoader, PMCFetcher
from .tsv import TSVFormatter
from .xml import EntityXMLFormatter, TextXMLFormatter
from .bioc import BioCXMLLoader, BioCJSONLoader, BioCXMLFormatter, BioCJSONFormatter
from .odin import ODINFormatter
from .brat import BratFormatter
from .becalm import BeCalmAbstractFetcher, BeCalmPatentFetcher
from .becalm import BeCalmTSVFormatter, BeCalmJSONFormatter
from .pubanno import PubAnnoJSONFormatter
from .pubtator import PubTatorLoader, PubTatorFBKLoader
from .pubtator import PubTatorFormatter, PubTatorFBKFormatter


# Keep these mappings up to date.
LOADERS = {
    'txt': TXTLoader,
    'txt_json': TXTJSONLoader,
    'bioc': BioCXMLLoader,  # keep for backwards compatibility
    'bioc_xml': BioCXMLLoader,
    'bioc_json': BioCJSONLoader,
    'becalmabstracts': BeCalmAbstractFetcher,
    'becalmpatents': BeCalmPatentFetcher,
    'pubmed': PXMLFetcher,
    'pubtator': PubTatorLoader,
    'pubtator_fbk': PubTatorFBKLoader,
    'pxml': PXMLLoader,
    'pxml.gz': MedlineLoader,
    'pmc': PMCFetcher,
    'nxml': PMCLoader,
}

INFMTS = list(LOADERS.keys())
INFMTS.remove('bioc')  # don't encourage obsolete names

EXPORTERS = {
    'tsv': TSVFormatter,
    'text_tsv': TSVFormatter,
    'xml': EntityXMLFormatter,
    'text_xml': TextXMLFormatter,
    'bioc': BioCXMLFormatter,  # keep for backwards compatibility
    'bioc_xml': BioCXMLFormatter,
    'bioc_json': BioCJSONFormatter,
    'odin': ODINFormatter,
    'brat': BratFormatter,
    'becalm_tsv': BeCalmTSVFormatter,
    'becalm_json': BeCalmJSONFormatter,
    'pubanno_json': PubAnnoJSONFormatter,
    'pubtator': PubTatorFormatter,
    'pubtator_fbk': PubTatorFBKFormatter,
}

OUTFMTS = list(EXPORTERS.keys())
OUTFMTS.remove('bioc')  # don't encourage obsolete names
