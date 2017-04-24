import gc
import re
import shutil
import string
from os import mkdir
from string import punctuation
import nltk
import regex
from nltk.tokenize import word_tokenize
import pickle

from ner.potential_ne import PotentialNE
from ner.rule import Rule
from tagger import Tagger
from nltk.tokenize.moses import MosesDetokenizer

class BuildRules(object):


    def __init__(self, stop_words, path_corpus, ngram, connector):
        try:
            # clean folder to keep rules' files
            shutil.rmtree('./rules')
        except FileNotFoundError:
            pass
        mkdir('./rules')

        self.stop_words = stop_words
        self.path_corpus = path_corpus
        self.ngram = ngram
        self.titles_punct = ['-', '–', ':', '?', '&', "'", '3D', '3d']
        self.end_punct = [punct for punct in punctuation if punct not in self.titles_punct]
        self.seed_items = []
        self.corpus_tags = {}

        self.db_connector = connector

        self.corpus_file = open(path_corpus, 'r', encoding='utf-8')

    def get_children_items(self, treated):

        return self.db_connector.get_potential_ne_where(['type', 'treated'], ['C', 0])

    def extract_rules(self, items_to_analyse, ne_type):
        """
        extracts the rules[left and right context] for a list of seed items and return them in an tuple of two arrays.
        In one array all the objects Rules have the same orientation[either left or right].
        :param write_files: array of string
        :return: array of Rules
        """
        try:

            if ne_type == 'S':
                self.seed_items.extend(items_to_analyse)

            for seed_item in items_to_analyse:

                first_set_rules = []

                # insert items in the database
                potential_ne = PotentialNE(seed_item, ne_type)
                potential_ne.idpotential_ne = self.db_connector.insert_potential_ne(potential_ne)

                # set reading point to zero
                self.corpus_file.seek(0)
                corpus_lines = self.corpus_file.readlines()

                total_lines = 0

                for line in corpus_lines:

                    total_lines += 1

                    if total_lines % 100 == 0:
                        print('Treating ' + str(potential_ne.surface) + ' - line: ' + str(total_lines))

                    # the escaped_seed_item is used to avoid the seed_item to be tokenized in during the line tokenization
                    escaped_potential_ne = potential_ne.get_escaped()

                    if escaped_potential_ne == '':
                        continue

                    # replace seed_item in the the line by the escaped_item
                    line = line.replace(potential_ne.surface, escaped_potential_ne)

                    if escaped_potential_ne not in nltk.word_tokenize(line, language='portuguese'):
                        continue

                    # chaque iteration append the first_set_rules with one array containing
                    # 4 items [left_rule, seed_item and right_rule, full_sentence]
                    parts = self._split_rule(line, escaped_potential_ne)

                    if len(parts) == 0:

                        parts = self._split_rule(line, '<begin>' + escaped_potential_ne)

                    first_set_rules.extend(parts)

                # save remaing rules
                self._save_rules_db(first_set_rules, potential_ne)

                gc.collect()

                # update potential_ne in the database
                potential_ne.treated = True
                self.db_connector.updated_potential_ne(potential_ne)

        except Exception:
            print('Problema extract_rules')
            return None

    def _save_rules_db(self, set_rules, potential_ne):
        try:

            for sentence_parts in set_rules:

                if len(sentence_parts) == 4:

                    sub_clause = self.has_subordinate_clause(sentence_parts[2])

                    if sub_clause is not None:
                        raw_rule_R_without_sub = self._validate_rule(sub_clause, self.ngram, 'R')

                        if raw_rule_R_without_sub is not None:
                            id_rule_R = self.db_connector.insert_rule(Rule(raw_rule_R_without_sub, 'R', sentence_parts[3],
                                                                           self.ngram, potential_ne))
                            self.db_connector.insert_relation_ne_rule(id_rule_R, potential_ne.idpotential_ne)

                    raw_rule_L = self._validate_rule(sentence_parts[0], self.ngram, 'L')
                    raw_rule_R = self._validate_rule(sentence_parts[2], self.ngram, 'R')

                    if raw_rule_L is not None:
                        id_rule_L = self.db_connector.insert_rule(Rule(raw_rule_L, 'L', sentence_parts[3],
                                                                       self.ngram, potential_ne))

                        if id_rule_L != -1:
                            self.db_connector.insert_relation_ne_rule(id_rule_L, potential_ne.idpotential_ne)

                    if raw_rule_R is not None:
                        id_rule_R = self.db_connector.insert_rule(Rule(raw_rule_R, 'R', sentence_parts[3], self.ngram,
                                                                       potential_ne))

                        if id_rule_R != -1:
                            self.db_connector.insert_relation_ne_rule(id_rule_R, potential_ne.idpotential_ne)

        except Exception:
            print('PROBLEM save_rule()')

    def _split_rule(self, sentence, escaped_potential_ne):
        """
        splits the sentence using the potential_ne passed as argument. It returns a list containing the sentence in
        parts, plus the full sentence in 3rd position.
        :param sentence: string sentence to be splitted
        :param escaped_potential_ne: potential_ne
        :return: list string containing parts of sentence
        """

        result = []

        parts = sentence.split(escaped_potential_ne)

        if len(parts) == 2:
            parts.insert(1, escaped_potential_ne)
            parts.append(sentence)
            result.append(parts)
            return result
        else:
            return []

    def has_subordinate_clause(self, part):
        """
        checks if the rule contains subordinate clauses. It returns the string without the subordinate conjunction, if
        it has a subordinate clause, and None if it hasn't subordinate clause.
        Ex. 'No filme Spider-Man, que estreou ontem...'
        :param part: string containing rule
        :return: string without subordinate clause
        """

        new_part = re.sub("(^,? (que|cuj[ao]|(n[ao]|d][oa]) qual))", "", part)

        if new_part == part:
            return None
        else:
            return new_part

    def extract_potential_nes(self):

        rules = self.db_connector.get_rules_where(['treated'], [0])

        for rule in rules:

            treated = []
            self.corpus_file.seek(0)
            corpus_lines = self.corpus_file.readlines()

            if(rule.surface.startswith('<begin>')):

                result_rule = self.__word_capital_letters_is_potNE("\n".join(corpus_lines), rule.surface)

                if result_rule is None:
                    # update rule value 'treated' for 'True' in database even if the rule is None
                    # it avoids this rule selected in next time we call db_connector.get_not_treated_rules()
                    rule.treated = 1
                    self.db_connector.update_rule(rule)
                    continue

            # array to keep potential NE found
            temp_results = []

            # iterate over all lines to extract potential NE associated to rule
            for index_line, line in enumerate(corpus_lines):

                if index_line in self.corpus_tags.keys():
                    POS = self.corpus_tags[index_line][0]
                    lemmas = self.corpus_tags[index_line][1]
                    tokens_treetagger = self.corpus_tags[index_line][2]
                else:
                    line = line.strip()

                    tagger = Tagger('portuguese', 'corpus_tagged.pk','/home/alexandre/treetagger/cmd/')
                    POS, lemmas, tokens_treetagger = tagger.tag_sentence(line)
                    self.corpus_tags[index_line] = [POS, lemmas,tokens_treetagger]

                joint_sent = "<sep>".join(lemmas)

                if rule.lemmas in joint_sent:

                    # get index to split rule
                    pot_ne_index = self._get_index_rule(rule, joint_sent, lemmas)

                    # obtain tokens using nltk - this tokens are used to compare with the tokens from treetagger
                    # this operation is important to obtain the correct index to split the sentence, because
                    # treetagger give tokenize the sentence with extra tokens splitting the workds like 'no', 'na'
                    # in 'em - o', 'em-a'

                    tokens_nltk = word_tokenize(line, language='portuguese')

                    # fix tokens problems in the NLTK tokenization
                    # tokens_nltk = []
                    # for token in tokens_nltk_temp:
                    #     if token == "sep_quotes":
                    #         tokens_nltk.append('"')
                    #     else:
                    #         tokens_nltk.append(token)

                    temp_results.append(self._split_potential_ne(tokens_nltk, tokens_treetagger, pot_ne_index, rule ))

                else:
                    continue # there's no rule occurrence in this the line

                treated.extend(self._save_potential_nes(rule, temp_results))

            # update treated status in the database
            rule.treated = 1
            self.db_connector.update_rule(rule)
            self._save_potential_nes(rule, temp_results)

    def _get_index_rule(self, rule, joint_sent, lemmas):
        """
        calculates the index where the sentence must to be splitted using the lemmas' rule.
        :param rule: object Rule
        :param joint_sent: string sentence lemmas joint by '<sep>
        :param lemmas:
        :return:
        """
        if rule.orientation == 'L':
            ne_context = joint_sent.split(rule.lemmas)[1]
            pot_ne_index = len(lemmas) - len(ne_context.split("<sep>")) + 1
        else:
            ne_context = joint_sent.split(rule.lemmas)[0]
            pot_ne_index = len(ne_context.split("<sep>")) - 1

        return pot_ne_index

    def _split_potential_ne(self, tokens_nltk, tokens_treetagger, pot_ne_index, rule ):
        """
        splits the sentence, using the rule as reference. It returns the context of the sentence where the potential ne
        may be. This functions compares the tokens produced by TreeTagger and NLTK to split to sentence in the right
        point, taking in account the difference of tokens between the two sets.
        Ex:
        rule.lemmas = 'o estreia de'
        sentence = 'Hoje foi a estreia de 'Star Wars" nos cinemas do Brasil'

        For the example above, the functions returns
        ['Star', 'Wars', 'nos', 'cinemas', do, 'Brasil']

        :param tokens_nltk_temp: sentence in tokens produce by nltk
        :param tokens_treetagger: sentence in tokens produce by treetagger
        :param pot_ne_index: int representing the point where the sentence must to be splitted
        :param rule: object Rule
        :return:
        """

        ##################################################################################################################
        tokens_nltk = [token.replace('sep_quotes', '"') for token in tokens_nltk]
        changes_limit = len(tokens_treetagger) - len(tokens_nltk)
        diff_index = 0
        control_index = 0
        control_changes = []

        while changes_limit > 0:
            for index, token in enumerate(tokens_nltk):
                index -= diff_index

                if tokens_nltk[index] != tokens_treetagger[index] and changes_limit > 0:
                    tokens_treetagger.pop(index)
                    changes_limit -= 1
                    diff_index += 1

                    control_changes.append(control_index)

                elif tokens_nltk[index] != tokens_treetagger[index]:
                    tokens_treetagger[index] = tokens_nltk[index]

                control_index += 1

        ##################################################################################################################

        # fix index to extract rule
        for index in control_changes:
            if index <= pot_ne_index:
                pot_ne_index -= 1

        if rule.orientation == 'L':
            pot_ne_context_tokens = tokens_treetagger[pot_ne_index:]
        else:
            pot_ne_context_tokens = tokens_treetagger[:pot_ne_index]

        # detokinze the context
        sent_detokenized = self._detokinze_tokens(pot_ne_context_tokens).replace(' QuotesL ', ' "').replace(' QuotesR ', '" ')

        return sent_detokenized

    def _detokinze_tokens(self, tokens):

        # treat quotes
        escaped_tokens = []
        count_quote = 1
        for word in tokens:

            if word == '"' and count_quote % 2 != 0:
                escaped_tokens.append('<open_quote>')
                count_quote += 1
            elif word == '"' and count_quote % 2 == 0:
                escaped_tokens.append('<close_quote>')
                count_quote += 1
            else:
                escaped_tokens.append(word)

        # pot_ne = tokens[pot_ne_index:]
        #
        detokenizer = MosesDetokenizer()
        sent_detokenized = detokenizer.detokenize(escaped_tokens, return_str=True)

        sent_detokenized = sent_detokenized.replace("<open_quote> ", '"')
        sent_detokenized = sent_detokenized.replace(" <open_quote>", '"')

        return sent_detokenized

    def _save_potential_nes(self, rule, temp_results):

        treated = []

        # check if the first set of rules has occurred with other elements, and not only the seed element
        for context_pot_ne in temp_results:

            potential_NE_valid = self._validate_ne(context_pot_ne, rule.orientation)

            if potential_NE_valid is not None and potential_NE_valid not in self.seed_items and potential_NE_valid not in treated:

                if ',' in potential_NE_valid:
                    potential_NE_valid = self._verify_ne_with_comma(potential_NE_valid, rule.orientation)
                    if potential_NE_valid is None:
                        continue

                if len(potential_NE_valid.split()) > 7:
                    potential_NE_valid = self._verify_long_ne(potential_NE_valid, rule.orientation)
                    if potential_NE_valid is None:
                        continue

                # check if the word is not only with capital letters, because it's in the beginning of sentence
                # if rule.orientation == 'R' and '<begin>' in potential_NE_valid:
                #     potential_NE_valid = self.__check_if_word_capital_letters_is_pot_ne("\n".join(corpus_lines),
                #                                                                         potential_NE_valid)
                #     if potential_NE_valid is None or potential_NE_valid == '':
                #         continue

                idpotential_ne = self.db_connector.insert_potential_ne(PotentialNE(potential_NE_valid, 'C'))
                self.db_connector.insert_relation_ne_rule(rule.idrules, idpotential_ne)
                treated.append(potential_NE_valid)

        return treated

    def _verify_long_ne(self, potential_NE, orientation):
        """
        verify if a long potential NE is composed for more than one potential NE. This function tries to divide it
        using the stop word as point of division.
        :param potential_NE:
        :param orientation:
        :return:
        """

        if potential_NE is None or orientation is None:
            return None

        with(open(self.path_corpus, 'r', encoding='utf-8')) as corpus_file:

            corpus = corpus_file.read()

            parts = []
            for stp_wd in self.stop_words:

                sub_part_with_sep = regex.sub(r'^(.+)\b' + stp_wd + r'\b(.+)$', r'\1<sep>\2', potential_NE)
                if '<sep>' in sub_part_with_sep:

                    if orientation == 'L':

                        parts.append(sub_part_with_sep.split('<sep>')[0])
                    else:
                        parts.append(sub_part_with_sep.split('<sep>')[1])

            freqs = []
            for index, part in enumerate(parts):

                part_valid = self._validate_ne(part, orientation)

                if part_valid is not None:

                    if part_valid != part:
                        # update the NE in the array, _validate_ne may have cleaned the NE
                        parts[index] = part_valid

                    # replace the tag <begin> and <end> to check the frequency
                    if part_valid.startswith('<begin>'):
                        part_valid = part_valid.replace('<begin>', '')

                    if part_valid.endswith('<end>'):
                        part_valid = part_valid.replace('<end>', '')

                    # check the frequency
                    freqs.append(len(regex.findall(r'\b' + regex.escape(part_valid) + r'\b', corpus)))
                else:
                    # the part is not a valid NE, set the frequency 0
                    freqs.append(0)

            # all parts have the same frequency, it's probably only one NE
            if len(set(freqs)) == 1 or len(freqs) < 2:

                # all the parts have the same frequency, get the longest one
                longest_NE = ''
                for part in parts:

                    if len(part) > len(longest_NE):
                        longest_NE = part

                return longest_NE

            # return item with the highest frequency
            max_freq = max(freqs)
            index_max_freq = freqs.index(max_freq)
            return parts[index_max_freq]

    def _verify_ne_with_comma(self, potential_NE, orientation):
        """
        checks if potential NEs containing comma can be split into small ones, according to its frequency.
        :param potential_NE:
        :param orientation:
        :return:
        """
        if potential_NE is None or orientation is None:
            return None

        with(open(self.path_corpus, 'r', encoding='utf-8')) as corpus_file:

            corpus = corpus_file.read()

            parts = potential_NE.split(',')

            freqs = []

            for part in parts: # todo verificar se a divisao entres os items dividos deve ser feita levando em conta a orientao da regra, como na funcao de NE longas

                part_valid = self._validate_ne(part, orientation)

                if part_valid is not None:

                    if part_valid.startswith('<begin>'):
                        part_valid = part_valid.replace('<begin', '')

                    freqs.append(len(regex.findall(r'\b' + regex.escape(part_valid) + r'\b', corpus)))
                else:
                    freqs.append(0)

            # all parts have the same frequency, it's probably only one NE
            if len(set(freqs)) == 1 or len(freqs) < 2:
                return potential_NE

            # parts have different frequency, get only the part concerned by the rule following the orientation
            if orientation.upper() == 'L':
                if freqs[0] > 0:
                    return parts[0]
            elif freqs[-1] > 0:
                return parts[-1]
            else:
                return potential_NE

    def _validate_ne(self, potential_NE, orientation):
        """
        validates the potential NE passed by argument eliminating tokens according to the rule orientation in order to
        obtain a clean potential Named Entity. If the string passed by argument doesn't contain a valid potential NE, it
        return None. The rule orientation is important, because the method will start to search invalid token by the
        left or the right according to the point where the string was extract from the text.
        Ex.
            blabla do Seed Item. [orientation R] 'blabla do' will be deleted
            Seed Item titi da [orientation L] 'titi da' will be deleted

        :param potential_NE: string containing a potential NE
        :param orientation: string indicating the orientation of the rule that extracted the potential NE.
        :return: string containg the potential ne without invalid token
        """

        # first criteria is to check if one our seed_items is present in the rule
        # we`ve choose seed items non-ambiguous, so it can`t occurs inside anoter NE
        # avoid bad matches like: No Seed Item, Nesse Seed Item

        if potential_NE is None or orientation is None:
            return None


        for seed in self.seed_items:
            if seed in potential_NE:
                return None

        replace_tag_begin = False

        if potential_NE.strip().startswith('<begin>'):
            potential_NE = potential_NE.strip().replace('<begin>', '')
            replace_tag_begin = True
        elif potential_NE.strip().endswith('<end>'):
            potential_NE = potential_NE.strip().replace('<end>', '')
            potential_NE = regex.sub('(.*?)(\.|\?|!|,|:|;|\+|\-|#|\)|\(|\]|\[|)$', r'\1', potential_NE).strip()

        ne = ''
        stop_count = 0

        potential_NE = potential_NE.strip()

        # control the use of punctuation
        punct_used = False

        if orientation == 'L':

            if len(potential_NE.strip()) > 0:
                # verify is NE starts with a capital letter
                if not potential_NE[0].isupper():
                    return None

            for token in nltk.word_tokenize(potential_NE):

                token = token.strip()

                #
                if regex.match('^.*?(\.|\(|\)|\]|\[|\}|\{|"|\').*$', token):
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

                if token in self.titles_punct and not punct_used:
                    ne += ' ' + token
                    punct_used = True  # allows only one punctuation mark in the title
                    continue

                if re.match('\d+', token):
                    ne += ' ' + token
                    continue

                # any of the criteria above has been filled
                # we passed the limit of NE
                if token[0].islower() or token in self.end_punct:
                    break
        else:
            for token in reversed(nltk.word_tokenize(potential_NE)):

                token = token.strip()

                if regex.match('^.*?(\.|\(|\)|\]|\[|\}|\{|"|\').*$', token):
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

                if token in self.titles_punct and not punct_used:
                    ne = token + ' ' + ne
                    punct_used = True  # allows only one punctuation mark in the title
                    continue

                if re.match('\d+', token):
                    ne = token + ' ' + ne
                    continue

                # any of the criteria above has been filled
                # we passed the limit of NE
                if token[0].islower() or token in self.end_punct:
                    break

        ne_cleaned = self.__clean_potential_ne(ne)

        if ne_cleaned.islower() or len(ne_cleaned.strip()) < 3:
            return None
        else:

            if replace_tag_begin:
                return '<begin>' + ne_cleaned.strip()
            else:
                return ne_cleaned.strip()

    def _validate_rule(self, raw_sub_string, ngram, orientation):
        """
        validates the rules extracted from a line. It receives a raw substring extracted from a line and deletes
        punctuation and unwanted characters to return a clean rule, respecting the tokens limit passed by arguments.
        Each rule is extracted according to its orientation. If a substring either doesn't contain a rule or contains
        an invalid rule, it returns None.
        :param raw_sub_string: string phrase extracted from the corpus where a seed item occurs
        :param ngram: rule size
        :param orientation: string representing the positing in regard to the seed item.
        :param stop_words: list of stop words
        :return: string clean rule
        """

        raw_sub_string = raw_sub_string.strip()

        if raw_sub_string is None or orientation is None or len(raw_sub_string) == 0:
            return None

        # check the rules starts or ends with punctuation
        if (raw_sub_string[0] in string.punctuation and orientation == 'R') or\
                (raw_sub_string[-1] in string.punctuation and orientation == 'L'):
            ngram += 1

        # get rule, according to ngram limit and orientation
        rule_parts = raw_sub_string.split(' ')
        if orientation == 'L':
            rule_temp = rule_parts[-ngram:]

        else:
            rule_temp = rule_parts[:ngram]
        raw_sub_string = " ".join(rule_temp)

        # check if the rules includes another sentence - eliminate others sentences parts
        punct_rx = re.match('.*?(\.|\?|!|:|;)+\s+[A-Z][a-z]*', raw_sub_string)

        if punct_rx is not None:

            if orientation == 'L':
                raw_sub_string = raw_sub_string.split(punct_rx.group(1))[1]
            else:
                raw_sub_string = raw_sub_string.split(punct_rx.group(1))[0]

        # check if the left rule contains another sentence (ending by punctuation)
        if re.match('.*?(\.|\?|!)+$', raw_sub_string) and orientation == 'L':
            return None

        # clean regex cache
        re.purge()

        return raw_sub_string

    def __clean_potential_ne(self, ne):

        # delete possible lower case stop words at the end or beginning of NE
        stop_w_regex = "|".join([str(r'\s' + re.escape(w) + r'\s').lower() for w in self.stop_words])

        ne_cleaned = regex.sub(r'^(\p{Ll}+.*?)(\p{Lu}+.*$)', r'\2', ne)

        #ne_cleaned = re.sub('[' + stop_w_regex + ']*?([A-Z].+)', r'\1', ne)
        ne_cleaned = re.sub('([A-Z].+?)\s[' + stop_w_regex + ']*?$', r'\1', ne_cleaned.strip())

        punct_regex = "|".join([re.escape(punct) for punct in punctuation])
        ne_cleaned = re.sub('[' + punct_regex + ']*?([A-Z].+)', r'\1', ne_cleaned)

        # fix punctuation with extra spaces
        ne_cleaned = re.sub('(.*?)\s(:|\?|!)(.*?)', r'\1' + r'\2' + r'\3', ne_cleaned)

        # eliminate trailing punctuation at the beginning of the string
        ne_cleaned = re.sub('^(–|:|\?|!|\.|\-|;|\)|\(|\]|\[)+(.*?)', r'\2', ne_cleaned.strip())

        # delete trailing punctuation at the end
        ne_cleaned = re.sub(r'(^.+?)(,|-|\)|\(|\.)+$', r'\1', ne_cleaned )

        # clean regex cache
        re.purge()

        return ne_cleaned

    def __check_if_word_capital_letters_is_pot_ne(self, corpus, rule):

        if corpus is None or rule is None:
            return None

        # match possibles NEs at the beginning of sentence ex.: <begin>Blabla do Blabla dodo extracts: Blabla do Blalbla
        regex_result = regex.match(r'<begin>(\p{Lu}+\p{Ll}*[\s|\-|:|\d+]*((\p{Ll}+|[\s|\-|:|\d+]+)*\p{Lu}+\p{Ll}*)*)', rule)

        # check if the first words starts with capital letter
        if regex_result is None:
            return rule.replace('<begin>', '') # first words starts with small case, return rule without begin tag

        first_word = regex_result.group(1)

        # remove trailing punctuation to count frequency of first_word
        first_word = re.sub(r'(.+?)(,|-|!|\?|\.)+$', r'\1', first_word).strip()

        # count frequency with capital letter
        freq_capital_letter = len(re.findall(r'\b' + first_word.strip() + r'\b', corpus))

        # count total frequency
        freq_total = len(re.findall(r'\b' + first_word.lower() + r'\b', corpus.lower()))

        try:
            # calculate the percentage of frequency the words is capitalize or not
            prctg_capt_freq = freq_capital_letter / freq_total

        except ZeroDivisionError:

            prctg_capt_freq = 0

        if prctg_capt_freq > 0.95:
            # first word is ne, return rule without this NE
            return rule.replace('<begin>', '')

        else:
            return None

    def __word_capital_letters_is_potNE(self, corpus, raw_rule, threshold_freq=0.95):
        """
        verifies with a word with capital letters after a tag <begin> is a potential NE, or it just a capitalized word
        in the beginning of sentence.
        :param corpus: string containing the corpus
        :param raw_rule: object rule
        :param threshold_freq float to determine the percentage of minimum frequency the word has occurs capitalized
        :return:
        """

        regex_result = regex.match(r'<begin>((\p{Lu}\p{Ll}+\s?)+)', raw_rule)

        # check if the first words starts with capital letter
        if regex_result is None:
            return raw_rule.replace('<begin>', '') # first words starts with small case, return rule without begin tag

        first_word = regex_result.group(1)

        # first words starts with capital letters, we clean it to count its frequency
        first_word = re.sub('.+?(,|-|!|\?|\.)', '', first_word).strip()


        # count frequency with capital letter
        freq_capital_letter = len(re.findall(r'\b' + first_word.strip() + r'\b', corpus))

        # count total frequency
        freq_total = len(re.findall(r'\b' + first_word.lower() + r'\b', corpus.lower()))

        try:
            # calculate the percentage of frequency the words is capitalize or not
            prctg_capt_freq = freq_capital_letter / freq_total

        except ZeroDivisionError:
            print('Error linha 477')
            prctg_capt_freq = 0

        if prctg_capt_freq > threshold_freq:
            # first word is ne, return rule without this NE
            rule_clean = raw_rule.replace(first_word, '').replace('<begin>', '')
        else:
            rule_clean = raw_rule.replace('<begin>', '')

        if len(rule_clean) == 0:
            return None
        else:
            return rule_clean
