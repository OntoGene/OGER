#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2018


'''
Client parameters accepted by the API.
'''


IN_PARAMS = [
    'include_mesh',
    'mesh_as_entities',
    'single_section',
    'sentence_split',
    'byte_offsets_out',
    'sentence_tokenizer',
    'field_names',
]
OUT_PARAMS = [
    'include_header',
    'sentence_level',
    'bioc_meta',
    'byte_offsets_in',
    'word_tokenizer',
    'field_names',
]


class ParamHandler:
    '''
    Separate input and output parameters.
    '''
    _targets = {}
    for group, label in ((IN_PARAMS, 'in'), (OUT_PARAMS, 'out')):
        for param in group:
            _targets.setdefault(param, []).append(label)

    def __init__(self, params):
        self.dict = params.get('dict')
        self.postfilters = params.getlist('postfilter')
        self._extracted = self._extract(params)

    @property
    def in_params(self):
        'Input parameters.'
        return self._extracted['in']

    @property
    def out_params(self):
        'Output parameters.'
        return self._extracted['out']

    def _extract(self, params):
        extracted = {'in': {}, 'out': {}}
        for param in params:
            canonical_name = param.replace('-', '_')  # allow dashes
            try:
                groups = self._targets[canonical_name]
            except KeyError:
                if param not in ('dict', 'postfilter'):
                    raise ValueError('unrecognised parameter: {}'.format(param))
            else:
                value = ' '.join(params.getlist(param))
                for group in groups:
                    extracted[group][canonical_name] = value

        return extracted
