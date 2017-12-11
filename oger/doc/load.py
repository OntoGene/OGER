#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Nico Colic, 2015--2017
# Modified by Adrian van der Lek and Lenz Furrer, 2016--2017


"""
Import articles/collections from various formats.
"""


import io
import codecs
import json
import gzip
from os.path import splitext, basename
import logging
from urllib import request as url_request, parse as url_parse

from lxml import etree

from .document import Collection, Article, Entity, EntityTuple
from ..util.iterate import peekaheaditer


def pxml_to_article(path_or_file, docid,
                    include_mesh=False, single_section=False):
    """Parse PubMed Abstract XML (Medline)."""
    tree = etree.parse(path_or_file)
    return _parsed_pxml_to_article(tree, docid, include_mesh=include_mesh,
                                   single_section=single_section)


def _parsed_pxml_to_article(tree, docid=None,
                            include_mesh=False, single_section=False):
    """Parse PubMed Abstract XML (Medline)."""
    # Get the PMID, if missing.
    if docid is None:
        docid = text_node(tree, './/PMID')

    article = Article(docid)
    sections = []

    # Most important stuff: title and abstract.
    title = ''.join(tree.find('.//ArticleTitle').itertext())  # markup in title
    article.add_section('Title', title + '\n')
    for abstract in tree.iterfind('.//AbstractText'):
        if abstract.text:
            # Sectioned abstracts have a label attribute.
            # Otherwise, use the containing elem's tag as the label
            # (usually "Abstract").
            label = abstract.get('Label')
            if label is None:
                label = abstract.getparent().tag
            sections.append((label, abstract.text + '\n'))

    # Add metadata if they can be found.
    article.year = text_node(tree, './/DateCompleted/Year')
    # There may be multiple publication types -- the first one is enough.
    article.type_ = text_node(tree, './/PublicationType')

    # Get the MeSH headings.
    if include_mesh:
        mesh_terms = tree.iterfind('.//MeshHeading/DescriptorName')
        mesh_terms = [elem.text + '\n' for elem in mesh_terms if elem.text]
        if mesh_terms:
            sections.append(('MeSH descriptor names', mesh_terms))

    if single_section:
        # Conflate the sections into one.
        # Put the section headers into the text (unless it is "UNLABELLED").
        # Append separators to each element.
        flat = []
        offset = article.subelements[-1].end
        for label, text in sections:
            if label not in ('UNLABELLED', 'Abstract'):
                flat.append((label + ': ', offset))
                offset += len(label) + 2
            if isinstance(text, str):
                sents = Article.tokenizer.span_tokenize_sentences(text, offset)
                flat.extend((sent, start) for sent, start, _ in sents)
                offset += len(text)
            else:
                # List of MeSH headings.
                for sent in text:
                    flat.append((sent, offset))
                    offset += len(sent)
        sections = [('Abstract', flat)]

    for label, text in sections:
        article.add_section(label, text)

    return article


def pxml_gz_to_articles(path_or_file, **kwargs):
    '''
    Parse gzipped Medline archives.
    '''
    with gzip.open(path_or_file, 'rb') as f:
        kwargs['tag'] = 'MedlineCitation'
        kwargs['converter'] = _parsed_pxml_to_article
        yield from _iterparse_xml(f, **kwargs)


def efetch_pxml(pmids, **kwargs):
    '''
    Get PXML abstracts from PubMed through the Entrez efetch interface.
    '''
    kwargs['db'] = 'pubmed'
    kwargs['tag'] = 'PubmedArticle'
    kwargs['converter'] = _parsed_pxml_to_article
    return efetch_articles(pmids, **kwargs)


def efetch_nxml(pmcids, **kwargs):
    '''
    Get PubMed Central full-text articles through the Entrez efetch interface.
    '''
    kwargs['db'] = 'pmc'
    kwargs['tag'] = 'article'
    kwargs['converter'] = _parsed_nxml_to_article
    return efetch_articles(pmcids, **kwargs)


def efetch_articles(ids, db, **kwargs):
    '''
    Get articles through the Entrez efetch interface.
    '''
    idlist = ','.join(ids)
    if not idlist:
        raise ValueError('Empty document-ID list.')
    query = url_parse.urlencode(dict(db=db, retmode='xml', id=idlist))
    logging.info("POST request to NCBI's efetch API with the query %r", query)
    req = url_request.Request(efetch_url, data=query.encode('ascii'))

    with url_request.urlopen(req) as f:
        yield from _iterparse_xml(f, **kwargs)

efetch_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'


def _iterparse_xml(stream, tag, converter, **kwargs):
    for _, a in etree.iterparse(stream, tag=tag):
        yield converter(a, **kwargs)
        a.clear()  # free memory


def nxml_to_article(path_or_file, docid):
    """Parse PubMed Central NXML."""
    return _parsed_nxml_to_article(etree.parse(path_or_file), docid)


def _parsed_nxml_to_article(tree, docid=None):
    """Convert XML downloaded from PMC to Article."""

    NL = "\n\n"

    title = tree.xpath('.//title-group/article-title')[0]
    title_str = ''.join(title.itertext()).strip() + NL

    # ABSTRACT
    abstract_str = ""
    for abstract in tree.xpath('.//abstract'):
        if abstract.get("abstract-type"):
            abstract_str += abstract.get("abstract-type").capitalize() + NL
            # This will allow titles like "teaser"
            # Not sure if CRAFT format allows this
            # Otherwise just set to "Abstract"
        else:
            abstract_str += "Abstract" + NL

        for abstract_section in abstract.xpath('.//title | .//p'):
            abstract_str += ''.join(abstract_section.itertext()).strip() + NL

    # BODY
    body_str = ""
    for body in tree.xpath('.//body'):
        for body_section in body.xpath('.//title | .//p | .//label'):
            body_str += ''.join(body_section.itertext()).strip() + NL

    # Try to get a missing PMCID.
    if docid is None:
        docid = tree.xpath('.//article-id[@pub-id-type="pmc"]')[0].text

    article = Article(docid)
    article.add_section('title', title_str)
    article.add_section('abstract', abstract_str)
    if body_str:
        article.add_section('body', body_str)

    return article


def text_node(tree_or_elem, xpath, default=None):
    '''
    Get the text node of the referenced element, or default.
    '''
    try:
        return tree_or_elem.find(xpath).text
    except AttributeError:
        return default


def becalm_abstracts(docids):
    '''
    Get abstracts from BeCalm's abstract server.
    '''
    return becalm_request('abstracts', docids, 'text')


def becalm_patents(docids):
    '''
    Get patents from BeCalm's patent server.
    '''
    return becalm_request('patents', docids, 'abstractText')


def becalm_request(domain, docids, textfield):
    '''
    Iterate over documents from the BeCalm servers.
    '''
    url = becalm_urls[domain]
    if not isinstance(docids, (tuple, list)):
        docids = list(docids)
    if not docids:
        raise ValueError('Empty doc-ID list.')
    query = json.dumps({domain: docids}).encode('ascii')
    headers = {'Content-Type': 'application/json'}
    logging.info("POST request to BeCalm's server with the query %s", query)
    req = url_request.Request(url, data=query, headers=headers)
    with url_request.urlopen(req) as f:
        docs = json.load(codecs.getreader('utf-8')(f))

    for doc in docs:
        id_ = doc['externalId']
        title = doc['title']
        text = doc[textfield]
        article = Article(id_)
        article.add_section('Title', title)
        article.add_section('Abstract', text)
        yield article

becalm_urls = {
    'abstracts': 'http://193.147.85.10:8088/abstractserver/json',
    'patents': 'http://193.147.85.10:8087/patentserver/json'
}


def txt_to_article(path_or_file, docid, **kwargs):
    '''
    Get a very simply structured article from plain text.
    '''
    if hasattr(path_or_file, 'read'):
        return open_txt_to_article(path_or_file, docid, **kwargs)
    with open(path_or_file, encoding='utf-8') as f:
        return open_txt_to_article(f, docid, **kwargs)


def open_txt_to_article(f, docid, single_section=False, sentence_split=False):
    '''
    Get a very simply structured article from a plain text stream.
    '''
    # Check if this stream needs decoding.
    if isinstance(f, (io.RawIOBase, io.BufferedIOBase)):
        f = codecs.getreader('utf-8')(f)

    if single_section:
        # All text in a single section.
        sections = [_reattach_blank(f)]
    else:
        # Sections are separated by blank lines.
        sections = []
        for line in _reattach_blank(f, signal_boundaries=True):
            if line is None:
                # Start a new section.
                sections.append([])
            else:
                sections[-1].append(line)

    if docid is None:
        # Resort to using the filename as an ID, if available.
        # (The stream might have an empty or no "name" attribute.)
        path = getattr(f, 'name', None) or 'unknown'
        docid = splitext(basename(path))[0]

    article = Article(docid)
    for text in sections:
        if not sentence_split:
            text = ''.join(text)
        article.add_section('', text)

    return article

def _reattach_blank(lines, signal_boundaries=False):
    '''
    Reattach blank lines to the preceding non-blank line.

    Initial blank lines are prepended to the first non-
    blank line.

    If signal_boundaries is True, the position of the blank
    lines is signaled through yielding None.
    This boundary is always signaled at the beginning, even
    if there are no leading blank lines.
    '''
    # Consume all lines until the first non-blank line was read.
    last = ''
    for line in lines:
        last += line
        if line.strip():
            break

    # Unless the input sequence is empty, the first signal is now due.
    if signal_boundaries and last:
        yield None

    # Continue with the rest of the lines.
    # The loop variable is always ahead of the yielded value.
    boundary = False
    for line in lines:
        if not line.strip():
            # Blank line. Don't yield anything, but set a flag for yielding
            # the signal after the current line was yielded.
            boundary = True
            last += line
        else:
            # Non-blank line. Yield what was accumulated.
            yield last
            last = line
            if signal_boundaries and boundary:
                yield None
                boundary = False

    # Unless the input sequence was empty, the last line is now due.
    if last:
        yield last


def bioc_to_collection(path_or_file, coll_id):
    '''
    Parse a BioC collection XML.
    '''
    return BioCReader.collection(path_or_file, coll_id)


class BioCReader(object):
    '''
    Parser for BioC XML.

    Currently, any existing relation nodes are discarded.
    '''

    @classmethod
    def collection(cls, path_or_file, coll_id):
        '''
        Read BioC XML into an article.Collection object.
        '''
        collection = Collection(coll_id)

        it = peekaheaditer(etree.iterparse(path_or_file, tag='document'))
        coll_node = next(it)[1].getparent()
        collection.metadata = cls.meta_dict(coll_node)

        for _, doc in it:
            collection.add_article(cls.article(doc))

        return collection

    @classmethod
    def article(cls, node):
        'Read a document node into an article.Article object.'
        article = Article(node.find('id').text)
        article.metadata = cls.infon_dict(node)
        article.year = article.metadata.pop('year', None)
        article.type_ = article.metadata.pop('type', None)
        for passage in node.iterfind('passage'):
            sec_type, text, offset, infon = cls.section(passage)
            article.add_section(sec_type, text, offset)
            article.subelements[-1].metadata = infon
            cls.insert_annotations(article.subelements[-1],
                                   passage.iterfind('.//annotation'))
            # Get infon elements on sentence level.
            for sent, sent_node in zip(article.subelements[-1],
                                       passage.iterfind('sentence')):
                sent.metadata = cls.infon_dict(sent_node)
        return article

    @classmethod
    def section(cls, node):
        'Get type, text and offset from a passage node.'
        infon = cls.infon_dict(node)
        type_ = infon.pop('type', None)
        offset = int(node.find('offset').text)
        text = text_node(node, 'text')
        if text is None:
            text = (cls.sentence(s) for s in node.iterfind('sentence'))
        return type_, text, offset, infon

    @classmethod
    def sentence(cls, node):
        'Get text and offset from a sentence node.'
        offset = int(node.find('offset').text)
        try:
            text = node.find('text').text
        except AttributeError:
            # No text node.
            text = ''
        return text, offset

    @classmethod
    def insert_annotations(cls, section, annotations):
        '''
        Add term annotations to the correct sentence.

        This method changes the section by side-effect.

        Any non-contiguous annotation is split up into
        multiple contiguous annotations.
        '''
        entities = []
        for anno in annotations:
            for loc in anno.iterfind('location'):
                start = int(loc.get('offset'))
                end = start + int(loc.get('length'))
                entities.append((start, end, anno))

        if not entities:
            return

        entities.sort(key=lambda e: e[:2])
        sentences = iter(section)
        try:
            sent = next(sentences)
            for start, end, anno in entities:
                while start >= sent.end:
                    sent = next(sentences)
                sent.entities.append(BioCAnno.entity(anno, start, end))
        except StopIteration:
            logging.warning('annotations outside character range')

    @classmethod
    def meta_dict(cls, node):
        'Read metadata into a dictionary.'
        meta = {n: node.find(n).text for n in ('source', 'date', 'key')}
        meta.update(cls.infon_dict(node))
        return meta

    @staticmethod
    def infon_dict(node):
        'Read all infon nodes into a dictionary.'
        return {n.attrib['key']: n.text for n in node.iterfind('infon')}


class BioCAnno(object):
    '''
    Converter for BioC annotation with cached attribute warnings.
    '''

    warned_already = set()

    @classmethod
    def entity(cls, anno, start, end):
        'Create an EntityTuple instance from a BioC annotation node.'
        id_ = anno.get('id')
        text = text_node(anno, 'text')
        info = cls.info(anno)
        return EntityTuple(id_, text, start, end, info)

    @classmethod
    def info(cls, anno):
        'Create an `info` tuple.'
        infons = BioCReader.infon_dict(anno)
        values = tuple(infons.pop(label, 'unknown')
                       for label in Entity.fields)
        for unused in infons:
            if unused not in cls.warned_already:
                logging.warning('ignoring BioC annotation attribute %s',
                                unused)
                cls.warned_already.add(unused)
        return values
