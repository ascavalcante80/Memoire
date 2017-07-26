import nltk
import regex
from tagger import Tagger
import re
from string import punctuation
from ner.potential_ne import PotentialNE
from collections import defaultdict
import operator

__author__ = 'alexandre s. cavalcante'

class Rule(object):

    def __init__(self, surface, orientation, sentence, ngram=None, potential_ne=None, treated=0):

        self.__titles_punct = ['-', ':', '?', '&', "'", '3D', '3d']
        self.__end_punct = [punct for punct in punctuation if punct not in self.__titles_punct]
        self.surface = surface
        self.orientation = orientation
        self.sentence = sentence
        self.ngram = ngram
        self.treated = treated

        if potential_ne is None or not isinstance(potential_ne, PotentialNE):
            self.POS = []
            self.lemmas = []
        else:
            # get tags and lemmas
            self.POS, self.lemmas = self.__get_tags(potential_ne)

        self.frequency = 0
        self.production = 0
        self.variety = 0
        self.seed_production = 0
        self.idrules = -1

    def get_escaped(self):

        try:

            surface_escaped = re.sub('(\?|:|;|!|,|\.)', '_', self.surface)

        except TypeError:
            print('error linha 379 ')

        surface_escaped = surface_escaped.replace(' ', '_')

        # replacign underscore in the beginning and in the end of string
        if re.match(r'^(_+)(.*?)$', surface_escaped):
            surface_escaped = re.sub(r'^(_+)(.*?)', r' \2', surface_escaped)

        if re.match(r'^(.*?)(_+)$', surface_escaped):
            surface_escaped = re.sub(r'^(.*?)(_+)$', r'\1 ', surface_escaped)

        return surface_escaped

    def get_potential_NE(self, text_portion):
        """
        extracts a potential Named Entity from a portion of text. The limit of this NE must be verified by in the corpus.
        :param text_portion: string text
        :return: string potential NE
        """

        ne = ''
        stop_count = 0

        if regex.match('.*?(\(|\)|\[|\]|\}|\{|\$|\_|\-|\d)+.*$', text_portion):
            return None

        if self.orientation == 'l':

            if len(text_portion.strip()) > 0:
                # verify is NE starts with a capital letter
                if not text_portion[0].isupper():
                    return None

            for token in text_portion.split(' '):

                token = token.strip()

                if token == '.':
                    break

                if len(token) < 1:
                    continue

                if token[0].isupper():
                    ne += ' ' + token
                    continue

                if token in self.stop_words and stop_count < 4:
                    ne += ' ' + token
                    stop_count += 1
                    continue

                if token in self.titles_punct:
                    ne += ' ' + token
                    continue

                if re.match('\d+', token):
                    ne += ' ' + token
                    continue

                # any of the criteria above has been filled
                # we passed the limit of NE
                if token[0].islower() or token in self.end_punct:
                    break
        else:
            for token in reversed(text_portion.split(' ')):

                token = token.strip()

                if token == '.':
                    break

                if len(token) < 1:
                    continue

                if token[0].isupper():
                    ne = token + ' ' + ne
                    continue

                if token in self.stop_words and stop_count < 4:
                    stop_count += 1
                    ne = token + ' ' + ne
                    continue

                if token in self.titles_punct:
                    ne = token + ' ' + ne
                    continue

                if re.match('\d+', token):
                    ne = token + ' ' + ne
                    continue

                # any of the criteria above has been filled
                # we passed the limit of NE
                if token[0].islower() or token in self.end_punct:
                    break

        ne_cleaned = self.__clean_potential_NE(ne)

        if ne_cleaned.islower() or len(ne_cleaned.strip()) < 3:
            return '*'
        else:
            return ne_cleaned.strip()
        # return ne

    def __get_set_of_NE(self, potential_NEs, orientation):
        set_of_NE = []
        for ne in potential_NEs:
            set_of_NE.append(self.get_potential_NE(ne))

        set_of_NE = [ne for ne in set_of_NE if ne is not None]
        total_of = len(set_of_NE)

        return set(set_of_NE), total_of

    def __clean_potential_NE(self, ne):


        # delete possible lower case stop words at the end or beginning of NE
        #
        stop_w_regex = "|".join([str(r'\s' + re.escape(w) + r'\s').lower() for w in self.stop_words])

        ne_cleaned = re.sub('[' + stop_w_regex + ']*?([A-Z].+)', r'\1', ne)
        ne_cleaned = re.sub('([A-Z].+?)\s[' + stop_w_regex + ']*?$', r'\1', ne_cleaned)

        punct_regex = "|".join([re.escape(punct) for punct in punctuation])
        ne_cleaned = re.sub('[' + punct_regex + ']*?([A-Z].+)', r'\1', ne_cleaned)

        # fix punctuation with extra spaces
        ne_cleaned = re.sub('(.*?)\s(:|\?|!)(.*?)', r'\1' + r'\2' + r'\3', ne_cleaned)

        return ne_cleaned

    def has_punctuation(self):

        if len(self.surface) == 0:
            return False

        if self.orientation.lower() == 'l' and self.surface[-1] in punctuation:
            return True
        elif self.orientation.lower() == 'r' and self.surface[0] in punctuation:
            return True
        else:
            return False

    def has_number(self):

        if re.match('.*?\d.*?', self.surface):
            return True
        else:
            return False

    def __get_tags(self, potential_ne):

        try:

            # avoid the potential_ne do be lemmatized
            self.sentence.line_escaped = self.sentence.line_escaped.replace(potential_ne.get_escaped(), 'POTENTIAL_NE')
            self.sentence.line_escaped = re.sub("(.+)([A-Z]+[a-z]+)(.*?)", r'\1ENTITY_REP\3', self.sentence.line_escaped)

            tree_tagger = Tagger('portuguese','corpus_tagged.pk', '/home/alexandre/treetagger/cmd/')

            POS, lemmas, tokens = tree_tagger.tag_sentence(self.sentence, False)
            try:
                index_potential_ne = lemmas.index('potential_ne')
            except ValueError:

                print('PROBLEM - get_tags: sentence:' + self.sentence.line_escaped)
                return [],[]

            if self.orientation == 'L':

                # avoid negative index and bad cut
                if index_potential_ne - self.ngram < 0:
                    POS = POS[:index_potential_ne]
                    lemmas = lemmas[:index_potential_ne]
                else:
                    POS = POS[index_potential_ne-self.ngram:index_potential_ne]
                    lemmas = lemmas[index_potential_ne-self.ngram:index_potential_ne]
            else:
                index_potential_ne += 1
                POS = POS[index_potential_ne:self.ngram + index_potential_ne]
                lemmas = lemmas[index_potential_ne:self.ngram + index_potential_ne]

            if potential_ne.ne_type == 'O':
                POS, lemmas = self.__del_articles(POS, lemmas)

            return POS, lemmas
        except Exception:
            print('PROBLEM - get_tags: sentence:' + self.sentence.line_escaped)
            return [],[]

    def __del_articles(self, POS, lemmas):
        # ------------------------------- treating ontology rule ---------------------------#
        # if it's an ontology rule, the articles in the end of rule oriented left must be deleted
        # in portuguese, in the news writing style, proper names don't take article. So they muste to be deleted
        # in order to make the rule works with NE's.

        if POS[-1].startswith('D'):
            POS = POS[:-1]
            lemmas = lemmas[:-1]

        elif '+D' in POS[:-1]:
            POS[:-1] = POS[-1].split('+')[0]
            lemmas[:-1] = lemmas[-1].split('+')[0]
        return POS, lemmas
