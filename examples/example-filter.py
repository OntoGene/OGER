#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Example postfilters.

To use the filters, run (for the first filter):

    oger run -p path/to/example-filter.py:remove_short [...]

More examples can be found in oger.post.*
'''


from oger.doc import document


def remove_short(content):
    '''
    Remove all annotations shorter than 3 characters.
    '''
    # Annotations are anchored at the sentence level.
    for sentence in content.get_subelements(document.Sentence):
        # Iterate over a copy, so the original entities list can be modified.
        for entity in list(sentence.entities):
            if entity.end-entity.start < 3:
                sentence.entities.remove(entity)


def change_origin(content):
    '''
    Change the original_resource field of every annotation to "Biogrid".
    '''
    # The entity infos are tuples, therefore changing a value means
    # replacing it with a modified copy.
    field = document.Entity.std_fields.index('original_resource')
    for entity in content.iter_entities():
        info = entity.info[:field] + ('Biogrid',) + entity.info[field+1:]
        entity.info = info
