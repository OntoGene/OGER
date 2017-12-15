#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2016


'''
Compute coverage of annotated terms with P/R/F1.
'''


import sys
import logging
import argparse
from collections import defaultdict


# craftanno2ogtsv output:
# doc start end term type concept ontology
#  1    2    3   4    5      6       7
GOLD_FIELDS = '1,5,2-4,6'

# pyogpipeline tsv output:
# doc type start end term _ concept _  _ ontology
#  1   2     3    4   5   6    7    8  9    10
ANNO_FIELDS = '1-5,7'


def main():
    '''
    Run as script.
    '''
    ap = argparse.ArgumentParser(
        description=__doc__,
        usage='%(prog)s -g PATH(s) -a PATH(s) [OPTIONS]')

    ip = ap.add_argument_group(
        title='input records')
    ip.add_argument(
        '-g', '--gold', nargs='+', required=True, metavar='PATH',
        help='path(s) to ground-truth file(s), or - to read from STDIN')
    ip.add_argument(
        '-a', '--anno', nargs='+', required=True, metavar='PATH',
        help='path(s) to annotation file(s), or - to read from STDIN')

    mp = ap.add_argument_group(
        title='measure',
        description='how to treat partial positives: strict|lenient|average '
                    '(default: strict) '
                    'how to average P/R/F1 across documents: micro|macro '
                    '(default: micro)')
    mp.add_argument(
        '--strict', dest='measure', action='store_const', const='strict',
        default='strict',
        help='partial matches are errors (FP/FN) (default)')
    mp.add_argument(
        '--lenient', dest='measure', action='store_const', const='lenient',
        default=argparse.SUPPRESS,
        help='partial matches are correct')
    mp.add_argument(
        '--average', dest='measure', action='store_const', const='average',
        default=argparse.SUPPRESS,
        help='partial matches are half correct, half errors')
    mp.add_argument(
        '--micro', dest='macro', action='store_false', default=False,
        help='treat all records as one large document (default)')
    mp.add_argument(
        '--macro', action='store_true', default=argparse.SUPPRESS,
        help='compute P/R/F1 separately for each document '
             '(cf. option --doc-id)')

    fp = ap.add_argument_group(
        title='field specifications',
        description='Specify which fields to use from the input TSV files. '
                    'Use a combination of commas and dashes to build a '
                    'list of indices, based at origin 1. Order is relevant. '
                    'Examples: "1,3,5", "2-4", "1,3-5,7-"')
    fp.add_argument(
        '-G', '--gold-fields', action='append', metavar='FIELDS',
        type=FieldSelector,
        help='relevant fields in the ground-truth TSV. '
             'By specifying the option multiple times, each input line '
             'produces multiple records. '
             '(default: "{}")'.format(GOLD_FIELDS))
    fp.add_argument(
        '-A', '--anno-fields', action='append', metavar='FIELDS',
        type=FieldSelector,
        help='relevant fields in the annotation TSV. '
             'The option may also be repeated. '
             'Make sure that the resulting field selection aligns with '
             'the gold fields. '
             '(default: "{}")'.format(ANNO_FIELDS))
    fp.add_argument(
        '-k', '--key-field', '--doc-id', type=int, metavar='N', default=1,
        help='by which field are the annotation records grouped? '
             '(avoids loading all annotations into memory '
             'when calculating recall) '
             '(default: %(default)s)')
    fp.add_argument(
        '-o', '--offset-fields', type=FieldSelector, metavar='START,END',
        default='3,4',
        help='which annotation fields contain the START and END offset? '
             '(required for the lenient and average measure only) '
             '(default: %(default)s)')

    op = ap.add_argument_group(
        title='output formatting options',
        description='Select any combination of the following output filters. '
                    'If the PATH argument is missing or "-", '
                    'the corresponding output is written to STDOUT.')
    op_params = dict(action='store_const', const='-', default=argparse.SUPPRESS)
    op.add_argument(
        '-p', '--precision', **op_params)
    op.add_argument(
        '-r', '--recall', **op_params)
    op.add_argument(
        '-f', '--f1', **op_params)
    op.add_argument(
        '-c', '--counts', help='TP/FP/FN[/PP] counts', **op_params)
    op_params.update(action='store', nargs='?', metavar='PATH')
    op.add_argument(
        '-t', '--tp', '--true-positives', **op_params)
    op.add_argument(
        '-s', '--fp', '--spurious', **op_params)
    op.add_argument(
        '-m', '--fn', '--misses', **op_params)
    op.add_argument(
        '-P', '--pp', '--partial', **op_params)
    op.add_argument(
        '--show-all', action='store_true',
        help='when printing TP/FP/FN/PP records, show all fields, '
             'not only the ones selected for comparison. '
             'This might increase the number of lines printed, '
             'without affecting P/R/F1 scores')

    args = ap.parse_args()
    # Argument post-checking.
    # The append action doesn't override default;
    # see https://bugs.python.org/issue16399
    if args.gold_fields is None:
        args.gold_fields = [FieldSelector(GOLD_FIELDS)]
    if args.anno_fields is None:
        args.anno_fields = [FieldSelector(ANNO_FIELDS)]
    args.key_field = args.anno_fields[0].map(args.key_field, origin=1)
    try:
        if len(args.offset_fields) != 2:
            raise ValueError()
    except ValueError:
        ap.error('argument -o/--offset-fields: '
                 'exactly 2 comma-separated integers required')
    else:
        # If they're needed, determine the offset indices in the selected view.
        if args.measure != 'strict':
            args.offset_fields = [args.anno_fields[0].map(f, origin=0)
                                  for f in args.offset_fields.indices]

    proc_from_fns(**vars(args))


def proc_from_fns(gold, gold_fields, anno, anno_fields, **params):
    '''
    Write the selected coverage items from parsed arguments.
    '''
    proc_from_lines(iterfiles(gold), gold_fields,
                    iterfiles(anno), anno_fields,
                    **params)


def proc_from_lines(g_lines, g_fields, a_lines, a_fields, show_all, **params):
    '''
    Get the coverage from iterables of lines.
    '''
    backmap = BackMap(show_all)
    return proc_from_contents(fieldselect(g_lines, g_fields, backmap.add_gold),
                              fieldselect(a_lines, a_fields, backmap.add_anno),
                              backmap=backmap, **params)


def proc_from_contents(gold, anno, measure='strict', macro=False,
                       key_field=0, offset_fields=(2, 3), backmap=None,
                       **out_params):
    '''
    Collect TP, FP, FN [and PP] from iterables of record tuples.

    Args:
        gold (iterable of tuples): ground truth
        anno (iterable of tuples): predicted annotations
        measure (str): one of ('strict', 'lenient', 'average')
        macro (bool): macro or micro averaging of P, R, F?
        key_field (int): primary sort key
        offset_fields (pair of int): where to find the offsets
        backmap (BackMap or None): pointers to original records
    '''
    writer = SelectionWriter(out_params, pp_counts=measure != 'strict')
    judge = Evaluator(gold, measure, key_field, offset_fields)
    if backmap is None:
        backmap = BackMap(enabled=False)
    if macro:
        macro = MacroAverager()

    for items in judge.itergroups(anno):
        writer.write_records(items, backmap)
        backmap.clear()
        if macro:
            macro.update(judge)

    if macro:
        counts = macro.totalcounts
        prf = macro.prf()
    else:
        counts = judge.counts
        prf = coverage_PRF(*judge.dist_pp())
    writer.write_coverage(prf)
    writer.write_counts(counts)
    writer.close()


class Evaluator(object):
    '''
    Divide into groups (documents) and determine TP/FP/FN/PP.
    '''

    # Constant: empty set.
    empty = frozenset()

    def __init__(self, gold, measure, key_field, offset_fields):
        self.gold = self._indexgold(gold, key_field)
        self.measure = measure
        self.key_field = key_field
        self.offset_fields = offset_fields

        # Counts: TP, FP, FN, PP
        self.counts = (0, 0, 0, 0)

    @staticmethod
    def _indexgold(items, n):
        'Index the items by the nth field.'
        index = defaultdict(set)
        for item in items:
            index[item[n]].add(item)
        return dict(index)

    def itergroups(self, anno):
        '''
        Collect TP/FP/FN per group.
        '''
        seen_groups = set()
        current_records = set()
        current_group = None
        for record in anno:
            group = record[self.key_field]
            if group != current_group:
                yield self._eval_group(current_group, current_records)
                if current_group in seen_groups:
                    logging.warning(
                        'Annotations of the same group (document) should be '
                        'in subsequent lines. Sort the records and/or specify '
                        'the group-ID column (option -k).')
                seen_groups.add(current_group)
                current_records.clear()
                current_group = group
            current_records.add(record)
        # Update with the last group, which hasn't been triggered yet.
        yield self._eval_group(current_group, current_records)
        seen_groups.add(current_group)
        # Iterate over groups missing entirely from the annotated data.
        for group in set(self.gold).difference(seen_groups):
            yield self._eval_group(group, self.empty)

    def _eval_group(self, group, anno):
        '''
        Get the TP/FP/FN/PP records of one group.
        '''
        gold = self.gold.get(group, self.empty)

        tp = gold.intersection(anno)
        fp = anno.difference(gold)
        fn = gold.difference(anno)
        pp = set()

        if self.measure != 'strict' and fn and fp:
            # Do this costly computation only if needed:
            # If fn or fp are empty, there's no chance of finding partials.
            for ann_record, gold_record in self._find_partials(fn, fp):
                # Move the inexact matches to the partial category.
                pp.add(ann_record)
                fp.remove(ann_record)
                fn.remove(gold_record)

        self._update_counts((tp, fp, fn, pp))
        return tp, fp, fn, pp

    def _find_partials(self, fn, fp):
        '''
        Find partial-positive records.

        Look for pairs of records among the FN and FP that
        have an offset overlap and match perfectly otherwise.

        Each record is paired at most once.
        Eg., if there is a 1:2 mapping, such as two distinct
        FP records that overlap with different parts of the
        same FN record, then only one of them is considered
        a PP, while the other one stays a FP.
        Which one is chosen in this case is arbitrary, but
        the choice is deterministic (the same across repeated
        runs).
        '''
        # Re-index the FN without offsets.
        gold = defaultdict(list)
        for record in fn:
            start, end, rest = self._decompose_by_offset(record)
            gold[rest].append((start, end, record))

        # In order not to miss potential pairings, sort all records by length.
        # If long-spanning records are paired first, they might shadow a sub-
        # sumed shorter match in some corner cases.
        # Eg., given FP = {'very long span', 'span'} and
        # FN = {'very', 'long span'} (offsets not shown):
        # If 'very long span' is first paired with 'long span', then the
        # remaining records won't match, while the inverse combination
        # would yield two pairs with no remainder.
        for occurrences in gold.values():
            occurrences.sort(key=self._lensort)
        fp = sorted((self._decompose_by_offset(record) + (record,)
                     for record in fp), key=self._lensort)

        # Look for any non-zero overlap.
        for start, end, rest, record in fp:
            for i, (g_start, g_end, g_record) in enumerate(gold.get(rest, ())):
                if max(start, g_start) < min(end, g_end):
                    del gold[rest][i]  # avoid another match
                    yield record, g_record
                    break

    @staticmethod
    def _lensort(start_end_rest):
        'Sort decomposed records by length, then position.'
        return start_end_rest[1]-start_end_rest[0], start_end_rest

    def _decompose_by_offset(self, record):
        'Separate offsets from the rest of the record.'
        start_field, end_field = self.offset_fields
        start, end, rest = None, None, []
        for i, item in enumerate(record):
            if i == start_field:
                start = int(item)
            elif i == end_field:
                end = int(item)
            else:
                rest.append(item)
        return start, end, tuple(rest)

    def _update_counts(self, records):
        'Update previous counts with the lengths of new items.'
        self.counts = tuple(c + len(r) for c, r in zip(self.counts, records))

    def dist_pp(self, reset=False):
        '''
        Distribute PP counts to TP/FP/FN according to the chosen measure.
        '''
        tp, fp, fn, pp = self.counts
        if self.measure == 'strict':
            # No need to add PP, since it's 0 anyway, due to optimisation.
            # (Otherwise, it would be added both to FP and FN.)
            counts = tp, fp, fn
        elif self.measure == 'lenient':
            # Count partial positives as true positives.
            counts = tp + pp, fp, fn
        elif self.measure == 'average':
            # Partial positives are half good, half bad.
            # Add the bad half to both FP and FN, so the denominator sums
            # of Precision and Recall both stay the same as with the other
            # measures.
            halfpp = pp / 2
            counts = tp + halfpp, fp + halfpp, fn + halfpp
        else:
            # Late sanity check.
            raise ValueError('invalid measure: {}'.format(self.measure))

        if reset:
            self.counts = (0, 0, 0, 0)

        return counts


class MacroAverager(object):
    '''
    Aggregator for macro-averaging P/R/F.
    '''
    def __init__(self):
        self.precision = []
        self.recall = []
        self.f1 = []
        self.totalcounts = (0, 0, 0, 0)  # TP, FP, FN, PP

    def update(self, judge):
        '''
        Update internal states and reset the Evaluator.
        '''
        self.totalcounts = tuple(
            p + c for p, c in zip(self.totalcounts, judge.counts))
        p, r, f = coverage_PRF(*judge.dist_pp(reset=True))
        self.precision.append(p)
        self.recall.append(r)
        self.f1.append(f)

    def prf(self):
        '''
        Compute average of P, R, F1.
        '''
        return tuple(sum(x)/len(x)
                     for x in (self.precision, self.recall, self.f1))


def coverage_PRF(tp, fp, fn):
    '''
    Compute P/R/F1.
    '''
    try:
        P = tp / (tp + fp)
    except ZeroDivisionError:
        # No FP -> perfect precision.
        P = 1
    try:
        R = tp / (tp + fn)
    except ZeroDivisionError:
        # No FN -> perfect recall.
        R = 1
    try:
        F1 = 2 * P * R / (P + R)
    except ZeroDivisionError:
        # Both P and R equal 0 -> F1 = 0
        F1 = 0
    return P, R, F1


def iterfiles(fns):
    'Iterate over lines of multiple files or STDIN.'
    if fns == ['-']:
        yield from sys.stdin
    else:
        for fn in fns:
            with open(fn, encoding='utf8') as f:
                yield from f


def fieldselect(lines, fieldsets, backmap_add):
    'Iterate over the selected fields from each line.'
    for line in lines:
        line = line.rstrip('\n\r').split('\t')
        for fields in fieldsets:
            selection = fields.select(line)
            backmap_add(selection, line)
            yield selection


class FieldSelector(object):
    '''
    Itemgetter with arbitrary indices and open end range.
    '''
    def __init__(self, exp):
        if isinstance(exp, str):
            self.indices, self.open_end = self._parse_indices(exp)
        else:
            self.indices, self.open_end = exp, None

    def __len__(self):
        if self.open_end is not None:
            raise ValueError('FieldSelector with indetermined length')
        return len(self.indices)

    @staticmethod
    def _parse_indices(exp):
        '''
        Parse a list of field index ranges.

        All indices are interpreted with origin 1 and
        converted to origin 0 internally.
        '''
        indices = []
        open_end = None
        # Handle open ranges at the start/end.
        if exp.startswith('-'):
            exp = '1' + exp
        rangelist = exp.split(',')
        if rangelist[-1].endswith('-'):
            open_end = int(rangelist.pop().strip('-')) - 1
        # Treat each list item as a range.
        for rng in rangelist:
            margins = rng.split('-')
            start = int(margins[0]) - 1
            end = int(margins[-1])
            indices.extend(range(start, end))
        return indices, open_end

    def select(self, items):
        'Select the predefined items from a sequence.'
        return tuple(self.iteritems(items))

    def iteritems(self, items):
        'Iterate over selected items.'
        for i in self.iterindices(len(items)):
            try:
                yield items[i]
            except IndexError:
                logging.warning(
                    'Too few input fields (expected at least %d): %s',
                    i+1, items)

    def iterindices(self, length):
        'Iterate over indices.'
        yield from self.indices
        if self.open_end is not None:
            yield from range(self.open_end, length)

    def map(self, index, origin=1):
        '''
        Map origin-based input index to its 0-based view position.

        In terms of the select() method:
        Map i to j, such that
            items[i-origin] == self.select(items)[j]
        '''
        index -= origin
        try:
            return self.indices.index(index)
        except ValueError:
            if self.open_end is not None and index >= self.open_end:
                return len(self.indices) + index - self.open_end
            else:
                raise


def BackMap(enabled=False):
    '''
    Create a memory for recovering complete lines from selected fields.

    In disabled mode, this returns a dummy object with no-op methods.
    '''
    if enabled:
        return _EnabledBackMap()
    else:
        return _DisabledBackMap()


class _EnabledBackMap(object):
    def __init__(self):
        self._anno = {}
        self._gold = {}
        self._last_addition = None  # see clear()

    def add_gold(self, selection, line):
        'Register a gold record.'
        self._add(self._gold, selection, line)

    def add_anno(self, selection, line):
        'Register an annotation record.'
        self._add(self._anno, selection, line)
        self._last_addition = selection

    @staticmethod
    def _add(memory, selection, line):
        try:
            memory[selection].append(line)
        except KeyError:
            memory[selection] = [line]

    def clear(self):
        'Reset the annotation memory.'
        carryover = self._anno.get(self._last_addition)
        self._anno.clear()

        # clear() is called when a group (document) is complete.
        # This is only known when the first line of the next group is
        # already consumed -- and that line has already been inserted
        # into this BackMap.
        # So we need to make sure it is still accessible in the next group.
        if self._last_addition is not None:
            self._anno[self._last_addition] = carryover
            self._last_addition = None

    def get(self, selection):
        '''
        Iterate over full lines.

        Try annotation records first.
        If that fails, go for gold records.
        '''
        try:
            yield from self._anno[selection]
        except KeyError:
            yield from self._gold[selection]


class _DisabledBackMap(_EnabledBackMap):
    @staticmethod
    def add_gold(selection, line):
        pass

    @staticmethod
    def add_anno(selection, line):
        pass

    @staticmethod
    def clear():
        pass

    @staticmethod
    def get(selection):
        yield selection


class SelectionWriter(object):
    '''
    Handler for writing selected portions.
    '''

    labels = ('tp', 'fp', 'fn', 'pp',
              'precision', 'recall', 'f1',
              'counts')

    def __init__(self, selection, pp_counts=False):
        self.record_selection = []
        self.coverage_selection = []
        self.count_stream = None
        self._open_streams = []
        self._count_labels = [l.upper() for l in self.labels[:3+pp_counts]]

        for i, label in enumerate(self.labels):
            try:
                path = selection[label]
            except KeyError:
                continue
            stream = self._open(path)
            if i < 4:
                self.record_selection.append((i, stream))
            elif i < 7:
                self.coverage_selection.append((i-4, label.title(), stream))
            else:
                self.count_stream = stream

    def write_records(self, items, backmap):
        'Write TP/FP/FN/PP records.'
        for i, stream in self.record_selection:
            for record in items[i]:
                for line in backmap.get(record):
                    print(*line, sep='\t', file=stream)

    def write_coverage(self, prf):
        'Write P/R/F1 figures.'
        for i, label, stream in self.coverage_selection:
            print(label, prf[i], sep='\t', file=stream)

    def write_counts(self, counts):
        'Write TP/FP/FN/PP counts.'
        if self.count_stream is not None:
            for label, count in zip(self._count_labels, counts):
                print(label, count, sep='\t', file=self.count_stream)

    def _open(self, path):
        if path == '-':
            return sys.stdout
        else:
            stream = open(path, 'w', encoding='utf8')
            self._open_streams.append(stream)
            return stream

    def close(self):
        'Close any open filehandles.'
        for stream in self._open_streams:
            stream.close()
