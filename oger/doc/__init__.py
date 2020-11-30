#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Internal text representation.
'''


from .txt import *
from .pubmed import *
from .tsv import *
from .xml import *
from .bioc import *
from .odin import *
from .brat import *
from .conll import *
from .becalm import *
from .pubanno import *
from .pubtator import *
from .europepmc import *


# Keep these mappings up to date.
LOADERS = {
    'txt': TXTLoader,
    'txt_json': TXTJSONLoader,
    'txt.tar': TXTTarLoader,
    'txt_tsv': TXTTSVLoader,
    'bioc': BioCXMLLoader,  # keep for backwards compatibility
    'bioc_xml': BioCXMLLoader,
    'bioc_json': BioCJSONLoader,
    'becalmabstracts': BeCalmAbstractFetcher,
    'becalmpatents': BeCalmPatentFetcher,
    'conll': CoNLLLoader,
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
    'txt': TXTFormatter,
    'text_tsv': TextTSVFormatter,
    'xml': EntityXMLFormatter,
    'text_xml': TextXMLFormatter,
    'bioc': BioCXMLFormatter,  # keep for backwards compatibility
    'bioc_xml': BioCXMLFormatter,
    'bioc_json': BioCJSONFormatter,
    'odin': ODINFormatter,
    'bionlp': DualFormatter,
    'bionlp.ann': BioNLPAnnFormatter,
    'brat': DualFormatter,
    'brat.ann': BratAnnFormatter,
    'conll': CoNLLFormatter,
    'becalm_tsv': BeCalmTSVFormatter,
    'becalm_json': BeCalmJSONFormatter,
    'pubanno_json': PubAnnoJSONFormatter,
    'pubanno_json.tgz': PubAnnoJSONtgzFormatter,
    'pubtator': PubTatorFormatter,
    'pubtator_fbk': PubTatorFBKFormatter,
    'europepmc': EuPMCFormatter,
    'europepmc.zip': EuPMCZipFormatter,
}

OUTFMTS = list(EXPORTERS.keys())
OUTFMTS.remove('bioc')  # don't encourage obsolete names
