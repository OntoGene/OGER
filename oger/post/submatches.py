#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2018--2019


'''
Postfilters for removing nested annotations.
'''


from collections import defaultdict
from typing import Sequence, Iterator

from ..doc import document


def remove_overlaps(content: document.Exporter) -> None:
    '''
    Remove annotations that partially overlap with another one.

    Of any cluster of overlapping spans, only the longest
    one(s) are kept.  Ties are not broken.

    Ignores entity type.
    '''
    _rm_any_overlaps(content, sametype=False, sub=False)


def remove_sametype_overlaps(content: document.Exporter) -> None:
    '''
    Remove same-type annotations that partially overlap with another one.

    Overlapping entities of different type are kept.
    '''
    _rm_any_overlaps(content, sametype=True, sub=False)


def remove_submatches(content: document.Exporter) -> None:
    '''
    Remove annotations that are contained in another one.

    Unlike `remove_overlaps()`, only true subsequences are removed.
    Consider the following annotation spans:
        |--------------|     (1)
        |--------|           (2)
                    |-----|  (3)
    This function removes only annotation (2), whereas
    `remove_overlaps` removes (2) and (3).
    '''
    _rm_any_overlaps(content, sametype=False, sub=True)


def remove_sametype_submatches(content: document.Exporter) -> None:
    '''
    Remove same-type annotations that are contained in another one.
    '''
    _rm_any_overlaps(content, sametype=True, sub=True)


def _rm_any_overlaps(content: document.Exporter, sametype: bool, sub: bool):
    if sametype:
        filter_ = _rm_sametype_overlaps
    else:
        filter_ = lambda e, s: list(_rm_overlapping(e, s))

    for sentence in content.get_subelements(document.Sentence):
        sentence.entities = filter_(sentence.entities, sub)


def _rm_sametype_overlaps(entities, sub):
    # Divide the entities into subsets by entity type.
    entity_types = defaultdict(list)
    for e in entities:
        entity_types[e.type].append(e)
    # Remove the submatches from each subset.
    filtered = []
    for e in entity_types.values():
        filtered.extend(_rm_overlapping(e, sub))
    filtered.sort(key=document.Entity.sort_key)
    return filtered


def _rm_overlapping(entities: Sequence[document.Entity],
                    sub: bool) -> Iterator[document.Entity]:
    '''
    Filter out annotations that overlap with others.
    '''
    # Get the indices of all removables.
    filter_ = _submatches if sub else _crossmatches
    removables = set(filter_(entities))
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
                # If the next entity will contain this one, then the previous
                # needs to be excluded as well.
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


def _crossmatches(entities):
    '''
    Identify partially overlapping entities to be excluded.
    '''
    for cluster in _clusters(entities):
        longest = max(l for _, l in cluster)
        for i, l in cluster:
            if l != longest:
                yield i

def _clusters(entities):
    cluster = []
    current_end = 0
    for i, e in enumerate(entities):
        if e.start >= current_end:
            if len(cluster) > 1:
                yield cluster
            cluster.clear()
        current_end = max(current_end, e.end)
        cluster.append((i, e.end-e.start))
    if len(cluster) > 1:
        yield cluster
