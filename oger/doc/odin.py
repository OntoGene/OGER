#!/usr/bin/env python3
# coding: utf8

# Author: Lenz Furrer, 2017


'''
Formatter for ODIN XML output.
'''


from lxml.builder import E

from .document import Collection, Entity
from .export import XMLMemoryFormatter


class ODINFormatter(XMLMemoryFormatter):
    '''
    ODIN's XML for text and annotations.
    '''
    section_names = {
        'title': 'article-title',
        'abstract': 'abstract',
        'mesh descriptor names': 'mesh',
    }

    def _dump(self, content):
        if isinstance(content, Collection):
            return self._collection(content)
        else:
            return self._article(content)

    def _collection(self, coll):
        node = E('collection')
        sent_id, tok_id = 0, 0  # continuous IDs
        for article in coll:
            art_node, sent_id, tok_id = self._article_counters(
                article, sent_id, tok_id)
            node.append(art_node)
        return node

    def _article(self, article):
        node, *_ = self._article_counters(article)
        return node

    def _article_counters(self, article, sent_id=0, tok_id=0):
        '''
        Article node and continuous IDs.
        '''
        node = E('article', id=str(article.id_))

        # Add sections.
        for section in article:
            sec_node, sent_id, tok_id = self._section(section, sent_id, tok_id)
            node.append(sec_node)

        # Add the (redundant?) stand-off annotations (concept level).
        node.append(self._og_dict(article))

        return node, sent_id, tok_id

    def _section(self, section, sent_id, tok_id):
        tag = self.section_names.get(section.type_.lower(), 'section')
        node = E(tag, id=str(section.id_))
        for sent_id, sent in enumerate(section, sent_id+1):
            sent_node, tok_id = self._sentence(
                sent, section.start, sent_id, tok_id)
            node.append(sent_node)
        return node, sent_id, tok_id

    def _sentence(self, sent, section_offset, sent_id, tok_id):
        sent_node = E('S', i=str(sent.id_), id='S{}'.format(sent_id))

        sent.tokenize()

        intertoks = self._itertoks(sent, tok_id, section_offset)
        mentions = self._itermentions(sent, section_offset)
        try:
            tok_id, token, tok_node = next(intertoks)
            for term_start, term_end, term_node in mentions:
                while token.end <= term_start:
                    # Tokens preceding this term mention are added directly
                    # to the sentence node.
                    sent_node.append(tok_node)
                    tok_id, token, tok_node = next(intertoks)

                # Tokens belonging to the term mention are added
                # to the mention's node.
                sent_node.append(term_node)
                while token.start < term_end:
                    term_node.append(tok_node)
                    tok_id, token, tok_node = next(intertoks)

                # Do some in-place modifications.
                self._fixup_term(term_node)

            sent_node.append(tok_node)

        except StopIteration:
            # Tokens exhausted inside a term mention
            # (or there is no text in this sentence).
            pass

        # Append any remaining tokens.
        for tok_id, _, tok_node in intertoks:  # redefine tok_id: up-reporting
            sent_node.append(tok_node)

        return sent_node, tok_id

    @staticmethod
    def _token(tok, cont_id, section_offset):
        '''
        Offsets restart at each section.
        '''
        node = E('W', tok.text,
                 id='W{}'.format(cont_id),
                 o1=str(tok.start - section_offset),
                 o2=str(tok.end - section_offset))
        return node

    def _itertoks(self, sent, tok_id, section_offset):
        '''
        Iterate over tokens with proper spacing (tail text).
        '''
        # We don't know if we need to append a space until we see the next
        # token. Therefore, yielding is delayed by one iteration.
        itoks = enumerate(sent, tok_id+1)
        try:  # respect PEP 479
            this_id, this_token = next(itoks)
        except StopIteration:
            return

        for next_id, next_token in itoks:
            node = self._token(this_token, this_id, section_offset)
            if next_token.start > this_token.end:
                # Non-adjacent tokens -> add a space.
                node.tail = ' '
            yield this_id, this_token, node
            this_id, this_token = next_id, next_token

        node = self._token(this_token, this_id, section_offset)
        yield this_id, this_token, node

    def _itermentions(self, sent, offset):
        '''
        Lump together overlapping term spans.
        '''
        last_end = 0
        mention = []
        for entity in sent.iter_entities():
            # Rely on the position-based sort order of the term mentions.
            if entity.start < last_end:
                # Overlapping term.
                mention.append(entity)
            else:
                # Disjunct term.
                if mention:
                    yield self._term_node(mention, offset)
                mention = [entity]
            last_end = max(entity.end, last_end)
        if mention:
            yield self._term_node(mention, offset)

    @staticmethod
    def _term_node(entities, offset):
        '''
        Produce a term node for co-located entity mentions.
        '''
        start = min(e.start for e in entities)
        end = max(e.end for e in entities)
        values = '|'.join('{}:{}:{}'.format(Entity.ID(e),
                                            Entity.TYPE(e),
                                            e.text)
                          for e in entities)
        type_ = '|'.join(set(Entity.TYPE(e) for e in entities))
        node = E('Term', allvalues=values, type=type_,
                 o1=str(start-offset), o2=str(end-offset))
        return start, end, node

    @staticmethod
    def _fixup_term(term_node):
        # Move the tail spacing from the last token to the term node.
        if len(term_node):
            # In some corner cases, term_node has no children.
            term_node.tail = term_node[-1].tail
            term_node[-1].tail = None
        # Add the ID attribute ex post
        # (now that the token IDs are known).
        tok_ids = (t.get('id') for t in term_node)
        term_node.set('id', 'T_{}'.format('_'.join(tok_ids)))

    @staticmethod
    def _og_dict(article):
        node = E('og-dict')
        seen = set()
        for entity in article.iter_entities():
            id_ = Entity.ID(entity)
            if id_ not in seen:
                node.append(E('og-dict-entry',
                              cid=str(id_),
                              cname=Entity.PREF(entity),
                              type=Entity.TYPE(entity)))
                seen.add(id_)
        return node
