#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
REST-specific export formats.
'''


from itertools import cycle
from collections import defaultdict

from ..doc import TSVFormatter, EntityXMLFormatter, BioCFormatter, ODINFormatter


class FurbishedODINFormatter(ODINFormatter):
    '''
    ODIN format with default term highlighting.
    '''
    def _dump(self, content):
        node = super()._dump(content)
        furbish_odin(node)
        return node


class CustomODINFormatter(ODINFormatter):
    '''
    ODIN format with a CSS href in the doctype declaration.
    '''
    doctype = '<?xml-stylesheet href="odin.css" type="text/css"?>'

    def _tostring(self, node, **kwargs):
        kwargs.setdefault('doctype', self.doctype)
        return super()._tostring(node, **kwargs)


EXPORTERS = {
    'tsv': TSVFormatter,
    'xml': EntityXMLFormatter,
    'bioc': BioCFormatter,
    'odin': FurbishedODINFormatter,
    'odin_custom': CustomODINFormatter,
}

EXPORT_FMTS = list(EXPORTERS)


def export(config, article, fmt):
    '''
    Export article to fmt, considering the settings in config.
    '''
    if fmt == 'tsv':
        content_type = 'text/tab-separated-values; charset=UTF-8'
    else:
        content_type = 'text/xml; charset=UTF-8'

    exporter = EXPORTERS[fmt](config, fmt)
    data = exporter.dump(article)
    return content_type, data


def furbish_odin(node):
    '''
    Add term highlighting.

    Add tooltip text showing the entity type.
    Use inline styles for bg-coloring the terms.
    (Inline styles are bad practice, but it's hard to do better
    without knowning the entity types in advance -- those can be
    customised at runtime by the user.)
    '''
    colors = cycle('cyan magenta gold chartreuse red lavender '
                   'yellowgreen orange skyblue grey'.split())
    entity_colors = defaultdict(lambda: next(colors))

    for term in node.iterfind('.//Term'):
        # Add tooltip text and background coloring.
        entity_type = min(term.get('type').split('|'), key=_type_priority)
        term.set('type', entity_type)  # update this attribute
        term.set('title', '\n'.join(term.get('allvalues').split('|')))
        color = entity_colors[entity_type]
        term.set('style', 'background-color: {}'.format(color))

    # Changes were made in-place, but return the node anyway.
    return node

def _type_priority(type_):
    '''
    Use a hard-coded priority for type disambiguation.
    '''
    # Unknown (=user-defined) types are favored over known ones.
    # Multiple unknown types are compared alphabetically.
    return type_priorities.get(type_, -1), type_

type_priorities = {
    t: i for i, t in enumerate((
        'biological_process',
        'molecular_function',
        'cellular_component',
        'cell',
        'disease',
        'chemical',
        'organism',
        'sequence',
        'cell_line',
        'gene/protein',
    ))
}
