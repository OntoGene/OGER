#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Example postfilters.

To use the filters, run (for the first filter):

    ./run.py -p path/to/example-filter.py:remove_short [...]
'''


from collections import defaultdict

import article


def remove_short(content):
    '''
    Remove all annotations shorter than 3 characters.
    '''
    # Annotations are anchored at the sentence level.
    for sentence in content.get_subelements(article.Sentence):
        # Iterate over a copy, so the original entities list can be modified.
        for entity in list(sentence.entities):
            if entity.end-entity.start < 3:
                sentence.entities.remove(entity)


def change_origin(content):
    '''
    Change the origin_db field of every annotation to "Biogrid".
    '''
    for entity in content.iter_entities():
        # entity.extra is a namedtuple, therefore changing a value means
        # replacing it with a modified copy.
        entity.extra = entity.extra._replace(original_resource='Biogrid')


def remove_submatches(content):
    '''
    Remove annotations that are contained in another one.

    Ignores entity type.
    '''
    for sentence in content.get_subelements(article.Sentence):
        sentence.entities = list(_remove_submatches(sentence.entities))


def remove_sametype_submatches(content):
    '''
    Remove same-type annotations that are contained in another one.

    Contained of different entity type are kept.
    '''
    entity_types = defaultdict(list)
    for sentence in content.get_subelements(article.Sentence):
        # Divide the entities into subsets by entity type.
        for e in sentence.entities:
            entity_types[e.extra.type].append(e)
        # Remove the submatches from each subset.
        filtered = []
        for entities in entity_types.values():
            filtered.extend(_remove_submatches(entities))
            entities.clear()  # entity_types is reused across sentences
        article.Entity.sort(filtered)
        sentence.entities = filtered


def _remove_submatches(entities):
    '''
    Filter out annotations that are contained in another one.
    '''
    # Get the indices of all submatches.
    removables = set(_submatches(entities))
    # Create a new, filtered list.
    return (e for i, e in enumerate(entities) if i not in removables)

def _submatches(entities):
    '''
    Identify all entities that are found within another entity.
    '''
    # Since the entities are sorted by offsets, only one reference is
    # needed for comparison.
    # However, runs of equal offsets might need to be excluded together --
    # when followed by a later entity which contains them all.
    ref_is, ref_entity = [], None
    for i, entity in enumerate(entities):
        if i:  # skip comparison in the first iteration (no reference yet)
            if _contains(ref_entity, entity):
                yield i
                continue  # keep the previous reference
            elif _contains(entity, ref_entity):
                yield from ref_is
            elif _equals(entity, ref_entity):
                # If the next entity will contain this one, then the previous needs
                # to be excluded as well.
                ref_is.append(i)
                continue  # keep the previous reference
        # If the current entity was not contained in the reference, then its
        # end offset is greater or equal to that of the reference.
        # Since the start offset of any future entity will not be lower than
        # the current one, we can safely update the reference.
        ref_is, ref_entity = [i], entity

def _contains(a, b):
    '''
    Return True if a contains b, False otherwise.
    '''
    return ((a.start <= b.start and a.end > b.end)
            or
            (a.start < b.start and a.end >= b.end))

def _equals(a, b):
    '''
    Return True if a's and b's offsets are the same.
    '''
    return a.start == b.start and a.end == b.end
