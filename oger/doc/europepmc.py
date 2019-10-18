#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2019


"""
Formatter for Europe PMC's JSON-lines format.
"""


__all__ = ['EuPMCFormatter', 'EuPMCZipFormatter']


import io
import json
import zipfile
import itertools as it

from .export import StreamFormatter


class EuPMCFormatter(StreamFormatter):
    """
    Formatter for Europe PMC's named-entity annotation format.
    """

    ext = 'jsonl'

    def write(self, stream, content):
        articles = content.get_subelements('Article', include_self=True)
        self._write(stream, articles)

    def _write(self, stream, articles):
        meta = self._metadata()
        for article in articles:
            doc = self._document(article, meta)
            if doc['anns']:
                json.dump(doc, stream)
                stream.write('\n')

    def _metadata(self):
        meta = self.config.p.eupmc_meta
        src = meta.get(
            'src', 'PMC' if self.config.p.article_format == 'pmc' else 'MED')
        provider = meta.get('provider', 'OGER')
        return {'src': src, 'provider': provider}

    def _document(self, article, meta):
        doc = dict(meta, id=article.id_, anns=[])
        text = article.text
        for s, sent in enumerate(article.get_subelements('Sentence'), start=1):
            section = self._section_name(sent.section, meta['src'])
            locations = it.groupby(sent.entities, key=lambda e: (e.start, e.end))
            for l, ((start, end), colocated) in enumerate(locations, start=1):
                types = it.groupby(colocated, key=lambda e: e.type)
                for type_, entities in types:
                    entities = set((e.pref, e.cid) for e in entities)
                    ann = {
                        'position': '{}.{}'.format(s, l),
                        'prefix': text[max(start-20, 0):start],
                        'postfix': text[end:end+20],
                        'exact': text[start:end],
                        'section': section,
                        'type': type_,
                        'tags': [{'name': n, 'uri': u} for n, u in entities]
                    }
                    doc['anns'].append(ann)
        return doc

    def _section_name(self, section, src):
        name = section.type_
        if src == 'PMC':
            if name not in self._pmc_sections:
                name = 'Article'
        elif name != 'Title':
            name = 'Abstract'
        return name

    _pmc_sections = frozenset((
        'Title', 'Abstract', 'Introduction', 'Methods', 'Results',
        'Discussion', 'Acknowledgments', 'References', 'Table', 'Figure',
        'Case study', 'Supplementary material', 'Conclusion', 'Abbreviations',
        'Competing Interests'))


class EuPMCZipFormatter(EuPMCFormatter):
    """
    Formatter for archives of Europe PMC's format.
    """

    ext = 'zip'
    binary = True

    def write(self, stream, content):
        articles = content.get_subelements('Article', include_self=True)
        # Iterate in hunks of 10,000, the max number of lines per file allowed.
        hunks = it.groupby(articles, key=lambda _, i=it.count(): next(i)//10000)

        with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as zf:
            for n, hunk in hunks:
                arcname = '{}_{}.jsonl'.format(content.id_, n+1)
                try:
                    member = zf.open(arcname, mode='w')
                except RuntimeError:  # Python < 3.6 doesn't support mode='w'
                    member = io.BytesIO()
                with io.TextIOWrapper(member, encoding='utf8') as f:
                    self._write(f, hunk)
                    if isinstance(member, io.BytesIO):
                        zf.writestr(arcname, member.getvalue())
