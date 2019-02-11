#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, 2015--2017
# Modified by Adrian van der Lek and Lenz Furrer, 2016--2017


'''
Loaders for different formats provided by PubMed.
'''


import gzip
import logging
import itertools as it
from urllib import request as url_request, parse as url_parse

from lxml import etree

from .document import Article, Entity
from .load import _Loader, DocLoader, DocIterator, text_node


class _MedlineParser(_Loader):
    '''
    Parser for PubMed abstracts in Medline's XML format.
    '''
    def _document(self, node, docid):
        # Get the PMID, if missing.
        if docid is None:
            docid = text_node(node, './/PMID')

        article = Article(docid, tokenizer=self.config.text_processor)
        # Add metadata if they can be found.
        article.year = text_node(node, './/DateCompleted/Year')
        # There may be multiple publication types -- the first one is enough.
        article.type_ = text_node(node, './/PublicationType')

        # Title.
        title = ''.join(node.find('.//ArticleTitle').itertext())
        article.add_section('Title', title + '\n')

        # Abstract body migt contain multiple sections, incl. a MeSH list.
        sections = self._iter_sections(node)

        if self.config.p.single_section:
            offset = article.subelements[-1].end
            sections = self._conflate_sections(sections, offset)

        anno_counter = it.count(1)
        for label, text, anno in sections:
            article.add_section(label, text)
            if any(anno):
                self._insert_annotations(article[-1], anno, anno_counter)

        return article

    def _conflate_sections(self, sections, offset):
        '''
        Conflate the sections into one.

        Put the section headers into the text (unless it is "UNLABELLED").
        Append separators to each element.
        '''
        # Temporary container for sentences, offsets and (optional) MeSH IDs.
        flat = []  # type: List[Tuple[Tuple[str, int], Optional[str]]]
        for label, text, anno in sections:
            if label not in ('UNLABELLED', 'Abstract'):
                flat.append(((label + ': ', offset), None))
                offset += len(label) + 2
            if isinstance(text, str):
                tok = self.config.text_processor
                sents = tok.span_tokenize_sentences(text, offset)
                flat.extend(((sent, start), None) for sent, start, _ in sents)
                offset += len(text)
            else:
                # List of MeSH headings.
                for sent, ui in zip(text, anno):
                    flat.append(((sent, offset), ui))
                    offset += len(sent)
        text, anno = zip(*flat) if flat else ((), ())
        yield 'Abstract', text, anno

    def _iter_sections(self, root):
        placeholder = [None]
        for section in root.iterfind('.//AbstractText'):
            # Sectioned abstracts have a label attribute.
            # Otherwise, use the containing elem's tag as the label
            # (usually "Abstract").
            text = ''.join(section.itertext())
            if not text:
                continue
            label = section.get('Label')
            if label is None:
                label = section.getparent().tag
            yield label, text + '\n', placeholder

        # Optionally add the MeSH list.
        add_anno = self.config.p.mesh_as_entities
        if add_anno or self.config.p.include_mesh:
            mesh = [(entry.text + '\n', entry.get('UI', 'unknown'))
                    for entry in root.iterfind('.//MeshHeading/DescriptorName')
                    if entry.text]
            if mesh:
                names, uis = zip(*mesh)
                if not add_anno:
                    uis = [None for _ in uis]
                yield 'MeSH descriptor names', names, uis

    def _insert_annotations(self, section, uis, counter):
        # Annotations come from the MeSH heading lists.
        # They are annotated at document level, but OGER needs character
        # offsets, so the names are included as text in a separate section
        # in order to serve as an anchor for the Entity objects.
        # Each descriptor name is included as a separate sentence.

        # When constructing the Article object, an ID is recorded for every
        # piece of text. It is a placeholder (None) most of the time.
        # This allows dealing with the complexity introduced with the
        # single-section option, which affects the offsets (among other things).

        for sent, ui in zip(section, uis):
            if ui is None:
                continue
            id_ = next(counter)
            text = sent.text.rstrip()
            start = sent.start
            end = start + len(text)
            info = self._entity_info(ui)
            sent.entities.append(Entity(id_, text, start, end, info))

    def _entity_info(self, native_id):
        info = ['unknown'] * len(self.config.entity_fields)
        info[2] = 'MeSH'
        info[3] = native_id
        return tuple(info)


class _PMCParser(_Loader):
    '''
    Parser for PubMed Central's full-text XML.
    '''
    NL = '\n\n'

    def _document(self, node, docid):
        title = self._itertext(node.find('.//title-group/article-title'))
        abstract = ''.join(self._get_abstract(node))
        body = ''.join(self._get_body(node))

        # Try to get a missing PMCID.
        if docid is None:
            docid = node.find('.//article-id[@pub-id-type="pmc"]').text

        article = Article(docid, tokenizer=self.config.text_processor)
        article.add_section('title', title)
        article.add_section('abstract', abstract)
        if body:
            article.add_section('body', body)

        return article

    def _get_abstract(self, root):
        for node in root.xpath('.//abstract'):
            if node.get("abstract-type"):
                yield node.get("abstract-type").capitalize() + self.NL
                # This will allow titles like "teaser"
                # Not sure if CRAFT format allows this
                # Otherwise just set to "Abstract"
            else:
                yield "Abstract" + self.NL

            for abstract_section in node.xpath('.//title | .//p'):
                yield self._itertext(abstract_section)

    def _get_body(self, root):
        for node in root.xpath('.//body'):
            for body_section in node.xpath('.//title | .//p | .//label'):
                yield self._itertext(body_section)

    def _itertext(self, node):
        return ''.join(node.itertext()).strip() + self.NL


class _IterparseLoader:
    '''
    Mix-in for lazily loading documents from a large XML.

    Subclasses must override the "tag" class attribute.
    '''
    tag = None

    def _iterparse(self, stream):
        for _, node in etree.iterparse(stream, tag=self.tag):
            yield self._document(node, None)
            node.clear()  # free memory

    def _document(self, node, docid):
        raise NotImplementedError()


class _NCBIFetcher(DocIterator, _IterparseLoader):
    '''
    Fetch documents from NCBI's efetch interface.

    Subclasses must override the "db" class attribute.
    '''
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
    db = None

    def iter_documents(self, source):
        '''
        Iterate over documents from NCBI.
        '''
        docids = ','.join(source)
        if not docids:
            raise ValueError('Empty document-ID list.')
        query = url_parse.urlencode(dict(db=self.db, retmode='xml', id=docids))
        logging.info(
            "POST request to NCBI's efetch API with the query %r", query)
        req = url_request.Request(self.url, data=query.encode('ascii'))

        with url_request.urlopen(req) as f:
            yield from self._iterparse(f)


class PXMLLoader(DocLoader, _MedlineParser):
    '''
    Loader for single-doc Medline XML (pxml).
    '''
    def document(self, source, id_):
        node = etree.parse(source)
        return self._document(node, id_)


class PMCLoader(DocLoader, _PMCParser):
    '''
    Loader for single-doc PMC full-text XML (nxml).
    '''
    def document(self, source, id_):
        node = etree.parse(source)
        return self._document(node, id_)


class PMCFetcher(_NCBIFetcher, _PMCParser):
    '''
    Loader for PMC full-text documents through efetch.
    '''
    db = 'pmc'
    tag = 'article'


class PXMLFetcher(_NCBIFetcher, _MedlineParser):
    '''
    Loader for PubMed abstracts through efetch.
    '''
    db = 'pubmed'
    tag = 'PubmedArticle'


class MedlineLoader(DocIterator, _MedlineParser, _IterparseLoader):
    '''
    Loader for gzipped collections of Medline abstracts.
    '''
    # Implementation note: behaves like a fetcher, but takes a filename
    # instead of an ID list.
    tag = 'MedlineCitation'

    def iter_documents(self, source):
        '''
        Iterate over documents from a gzipped Medline collection.
        '''
        with gzip.open(source, 'rb') as f:
            yield from self._iterparse(f)
