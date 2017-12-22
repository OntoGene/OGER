#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Loader base classes.
'''


from .document import Collection


class _Loader:
    '''
    Abstract base loader.

    Subclasses must implement load_one().
    '''
    def __init__(self, config):
        self.config = config

    def load_one(self, source, id_):
        '''
        Load a single content (Article or Collection).
        '''
        raise NotImplementedError()


class DocLoader(_Loader):
    '''
    Load a single document at a time.

    Subclasses must implement document().
    '''
    def load_one(self, source, id_):
        return self.document(source, id_)

    def document(self, source, id_):
        '''
        Load a single document.
        '''
        raise NotImplementedError()


class CollLoader(_Loader):
    '''
    Load a whole collection of documents.

    Subclasses must implement collection().
    '''
    def load_one(self, source, id_):
        return self.collection(source, id_)

    def collection(self, source, id_):
        '''
        Load a complete collection.
        '''
        raise NotImplementedError()

    def iter_documents(self, source):
        '''
        Iterate over the documents of a collection.
        '''
        yield from self.collection(source, id_=None)


class DocIterator(_Loader):
    '''
    Load multiple documents from a single source.

    Subclasses must implement iter_documents().
    '''
    def load_one(self, source, id_):
        docs = self.iter_documents(source)
        return Collection.from_iterable(docs, id_)

    def iter_documents(self, source):
        '''
        Iterate over all documents.
        '''
        raise NotImplementedError()


def text_node(tree_or_elem, xpath, onerror=None, ifnone=None):
    '''
    Get the text node of the referenced element.

    If the node cannot be found, return `onerror`:
    If the node is found, but its text content is None,
    return ifnone.
    '''
    try:
        text = tree_or_elem.find(xpath).text
    except AttributeError:
        text = onerror
    else:
        if text is None:
            text = ifnone
    return text
