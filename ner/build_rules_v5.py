import gc
import operator
import re
import shutil
import string
from os import mkdir
from string import punctuation

import nltk
import regex

from database.mySQLConnector import MySQLConnector
from ner.potential_ne import PotentialNE
from ner.rule import Rule


class BuildRules(object):


    def __init__(self, stop_words, path_corpus, ngram):
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

        self.__conn = MySQLConnector()


    def extract_rules(self, seed_items, is_seed=True, is_ontology_type=False):
        """
        extracts the rules[left and right context] for a list of seed items and return them in an tuple of two arrays.
        In one array all the objects Rules have the same orientation[either left or right].
        :param write_files: array of string
        :return: array of Rules
        """

        if is_seed:
            self.seed_items.extend(seed_items)

        for seed_item in seed_items:
            pott_NE = PotentialNE(seed_item, is_seed)
            pott_NE.id = self.__conn.insert_potential_ne(pott_NE)

            # open corpus to read
            with (open(self.path_corpus, 'r', encoding='utf-8')) as corpus_file:

                corpus_file.seek(0) # set reading point to zero
                corpus_lines = corpus_file.readlines()

                # the escaped_seed_item is used to avoid the seed_item to be tokenized in during the line tokenization
                escaped_seed_item = self._escape_seed_item(seed_item)

                if escaped_seed_item == '':
                    continue

                first_set_rules = []

                for line in corpus_lines:

                    if seed_item not in line:
                        continue

                    # replace seed_item in the the line by the escaped_item
                    line = line.replace(seed_item, escaped_seed_item)

                    # chaque iteration append the first_set_rules with one array containing 3 items [left_rule, seed_item and right_rule]
                    parts = self.split_simple(line, escaped_seed_item)
                    if len(parts) == 0:

                        parts = self.split_simple(line, '<begin>' + escaped_seed_item)

                    # parts.append(line)
                    first_set_rules.extend(parts)


            gc.collect()

            if is_ontology_type:
                self._save_rules_ontology(first_set_rules, pott_NE)
            else:
                self._save_rules_DB(first_set_rules, pott_NE)

            pott_NE.treated = True
            self.__conn.updated_potential_NE(pott_NE)


    def extract_ontology_rules(self, ontology_items):
        """
        extracts the rules[left and right context] for a list of seed items and return them in an tuple of two arrays.
        In one array all the objects Rules have the same orientation[either left or right].
        :param write_files: array of string
        :return: array of Rules
        """


        for ontology_item in ontology_items:
            pott_NE = PotentialNE(ontology_item, True)
            pott_NE.id = self.__conn.insert_potential_ne(pott_NE)

            # open corpus to read
            with (open(self.path_corpus, 'r', encoding='utf-8')) as corpus_file:

                corpus_file.seek(0) # set reading point to zero
                corpus_lines = corpus_file.readlines()

                # the escaped_seed_item is used to avoid the seed_item to be tokenized in during the line tokenization
                escaped_seed_item = self._escape_seed_item(ontology_item)

                if escaped_seed_item == '':
                    continue

                first_set_rules = []

                for line in corpus_lines:

                    # for the ontology, we use all the line in lower case
                    line = line.lower()

                    if ontology_item not in nltk.word_tokenize(line):
                        continue

                    # replace seed_item in the the line by the escaped_item
                    line = line.replace(ontology_item, escaped_seed_item)

                    # chaque iteration append the first_set_rules with one array containing 3 items [left_rule, seed_item and right_rule]
                    parts = self.split_simple(line, escaped_seed_item)
                    if len(parts) == 0:

                        parts = self.split_simple(line, '<begin>' + escaped_seed_item)

                    # parts.append(line)
                    first_set_rules.extend(parts)


            gc.collect()

            self._save_rules_ontology(first_set_rules, pott_NE)

            pott_NE.treated = True
            self.__conn.updated_potential_NE(pott_NE)

    def _save_rules_ontology(self, set_rules, pott_NE):

        for rules in set_rules:
            if len(rules) == 4:

                sub_clause = self.has_subordinate_clause(rules[2])

                if sub_clause is not None:
                    raw_rule_R_without_sub = self._validate_rule(sub_clause, self.ngram, 'R')

                    if raw_rule_R_without_sub is not None:
                        id_rule_R = self.__conn.insert_rule_ontology(Rule(raw_rule_R_without_sub, 'R', rules[3]))
                        self.__conn.insert_relation_ne_rule(id_rule_R, pott_NE.id)

                raw_rule_L = self._validate_rule(rules[0], self.ngram, 'L')
                raw_rule_R = self._validate_rule(rules[2], self.ngram, 'R')

                if raw_rule_L is not None:
                    id_rule_L = self.__conn.insert_rule_ontology(Rule(raw_rule_L, 'L', rules[3]))
                    self.__conn.insert_relation_ne_rule(id_rule_L, pott_NE.id)

                if raw_rule_R is not None:
                    id_rule_R = self.__conn.insert_rule_ontology(Rule(raw_rule_R, 'R', rules[3]))
                    self.__conn.insert_relation_ne_rule(id_rule_R, pott_NE.id)


    def _save_rules_DB(self, set_rules, pott_NE):

        for rules in set_rules:
            if len(rules) == 4:

                sub_clause = self.has_subordinate_clause(rules[2])

                if sub_clause is not None:
                    raw_rule_R_without_sub = self._validate_rule(sub_clause, self.ngram, 'R')

                    if raw_rule_R_without_sub is not None:
                        id_rule_R = self.__conn.insert_rule(Rule(raw_rule_R_without_sub, 'R', rules[3]))
                        self.__conn.insert_relation_ne_rule(id_rule_R, pott_NE.id)

                raw_rule_L = self._validate_rule(rules[0], self.ngram, 'L')
                raw_rule_R = self._validate_rule(rules[2], self.ngram, 'R')

                if raw_rule_L is not None:
                    id_rule_L = self.__conn.insert_rule(Rule(raw_rule_L, 'L', rules[3]))
                    self.__conn.insert_relation_ne_rule(id_rule_L, pott_NE.id)

                if raw_rule_R is not None:
                    id_rule_R = self.__conn.insert_rule(Rule(raw_rule_R, 'R', rules[3]))
                    self.__conn.insert_relation_ne_rule(id_rule_R, pott_NE.id)


    def split_simple(self, line, joker):

        result = []

        parts = line.split(joker)

        if len(parts) == 2:
            parts.insert(1, joker)
            parts.append(line)
            result.append(parts)
            return result
        else:
            return []


    def get_potential_NEs(self):

        rules = self.__conn.get_not_treated_rules()

        with (open(self.path_corpus, 'r', encoding='utf-8')) as corpus_file:

            for rule in rules:

                treated = []
                corpus_file.seek(0)
                corpus_lines = corpus_file.readlines()

                if(rule.surface.startswith('<begin>')):

                    result_rule = self.__word_capital_letters_is_potNE("\n".join(corpus_lines), rule.surface)

                    if result_rule is None:
                        # update rule value 'treated' for 'True' in database even if the rule is None
                        # it avoids this rule selected in next time we call __conn.get_not_treated_rules()
                        rule.treated = True
                        self.__conn.updated_rule(rule)
                        continue

                # update treated status in the database
                rule.treated = True
                self.__conn.updated_rule(rule)

                # escape rule to replace it in the line and avoid tokenization
                rule_concat = self._escape_seed_item(rule.surface)

                # array to keep potential NE found
                temp_results = []

                # iterate over all lines to extract potential NE associated to rule
                for line in corpus_lines:

                    line = "<begin>" + line + "<end>"

                    # to normalize rules in the line, we compare in lower case
                    if rule.surface.lower() in line or rule.surface in line:

                        # replace rule by rule_concat in the line
                        line = line.replace(rule.surface.lower(), rule_concat)
                        line_lower = line.replace(rule.surface, rule_concat)

                        temp_results.extend(self.split_simple(line, rule_concat))
                        temp_results.extend(self.split_simple(line_lower, rule_concat.lower()))

                    else:
                        continue # there's no rule occurrence in this the line

                # get the potential NE index according to the orientation
                if rule.orientation == 'L':
                    potential_NE_index = 2
                else:
                    potential_NE_index = 0

                # check if the first set of rules has occurred with other elements, and not only the seed element
                for potential_NE in temp_results:

                    # this tuple must to have at least 3 items
                    if len(potential_NE) == 3:

                        potential_NE_valid = self._validate_NE(potential_NE[potential_NE_index], rule.orientation)

                        if potential_NE_valid is not None and potential_NE_valid not in self.seed_items and potential_NE_valid not in treated:

                            if ',' in potential_NE_valid:
                                potential_NE_valid = self._verify_ne_with_comma(potential_NE_valid, rule.orientation)
                                if potential_NE_valid is None:
                                    continue

                            if len(potential_NE_valid.split()) > 7:
                                potential_NE_valid = self._verify_long_NE(potential_NE_valid, rule.orientation)
                                if potential_NE_valid is None:
                                    continue

                            # check if the word is not only with capital letters, because it's in the beginning of sentence
                            if rule.orientation == 'R' and '<begin>' in potential_NE_valid:
                                potential_NE_valid = self.__check_if_word_capital_letters_is_potNE("\n".join(corpus_lines), potential_NE_valid)
                                if potential_NE_valid is None or potential_NE_valid == '':
                                    continue

                            potential_NE_id = self.__conn.insert_potential_ne(PotentialNE(potential_NE_valid))
                            self.__conn.insert_relation_ne_rule(rule.rule_id, potential_NE_id)
                            treated.append(potential_NE_valid)


    def _verify_long_NE(self, potential_NE, orientation):
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

                part_valid = self._validate_NE(part, orientation)

                if part_valid is not None:

                    if part_valid != part:
                        # update the NE in the array, _validate_NE may have cleaned the NE
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

            # all parts have the same freq, it's probably only one NE
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

                part_valid = self._validate_NE(part, orientation)

                if part_valid is not None:

                    if part_valid.startswith('<begin>'):
                        part_valid = part_valid.replace('<begin', '')

                    freqs.append(len(regex.findall(r'\b' + regex.escape(part_valid) + r'\b', corpus)))
                else:
                    freqs.append(0)

            # all parts have the same freq, it's probably only one NE
            if len(set(freqs)) == 1 or len(freqs) < 2:
                return potential_NE

            # parts have different freq, get only the part concerned by the rule following the orientation
            if orientation.upper() == 'L':
                if freqs[0] > 0:
                    return parts[0]
            elif freqs[-1] > 0:
                return parts[-1]
            else:
                return potential_NE


    def get_potential_NE_not_treated(self):

        not_treated_NEs = self.__conn.get_not_treated_NE()
        return not_treated_NEs


    def _validate_NE(self, potential_NE, orientation):
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

                if regex.match('^.*?(\.|\(|\)|\]|\[|\}|\{).*$', token):
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

                if regex.match('^.*?(\.|\(|\)|\]|\[|\}|\{).*$', token):
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

        ne_cleaned = self.__clean_potential_NE(ne)

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
        Each rule is extracted according to its orientation. If a substring either doesn't contain a rule or contains an invalid
         rule, it returns None.
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
        if (raw_sub_string[0] in string.punctuation and orientation == 'R')or (raw_sub_string[-1] in string.punctuation and orientation == 'L'):
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


    def has_subordinate_clause(self, part):
        """
        check if the rule contains subordinate clauses. It returns the string without the subordinate conjunction if it
        has a subordinate clause and None if it hasn't.
        :param part: string containing a rule
        :return: string without subordinate clause
        """

        new_part = re.sub("(^,? (que|cuj[ao]|(n[ao]|d][oa]) qual))", "", part)

        if new_part == part:
            return None
        else:
            return new_part


    def _escape_seed_item(self, seed_item):

        try:

            seed_item_clean = re.sub('(\?|:|;|!|,|\.)', '_', seed_item)

        except TypeError:
            print('error linha 379 ')

        seed_item_clean = seed_item_clean.replace(' ', '_')

        # replacign underscore in the beginning and in the end of string
        if re.match(r'^(_+)(.*?)$', seed_item_clean):
            seed_item_clean = re.sub(r'^(_+)(.*?)', r' \2', seed_item_clean)

        if re.match(r'^(.*?)(_+)$', seed_item_clean):
            seed_item_clean = re.sub(r'^(.*?)(_+)$', r'\1 ', seed_item_clean)

        return seed_item_clean


    def __clean_potential_NE(self, ne):

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


    def __check_if_word_capital_letters_is_potNE(self, corpus, rule):

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


    def calculate_frequencies(self):

        set_NEs = self.__conn.get_all_NE()

        with(open(self.path_corpus, 'r', encoding='utf-8')) as corpus_file:

            corpus = corpus_file.read()
            dic_freq =  {}
            for ne in set_NEs:
                freq = regex.findall(r'\b' + ne[0] + r'\b', corpus)
                dic_freq[ne[0]] = len(freq)
        dic2 = sorted(dic_freq.items(), key=operator.itemgetter(1))

        for ne_freq in dic2 :
            print(ne_freq)


    def get_items_production(self):
        """
        this functions returns a dictionary containing all NEs as keys, as value, this dictionary has arrays containing
        all the others NEs produced by the same rules where this NE key occurs.
        :return: dictionary of NEs
        """

        set_NEs = self.__conn.get_all_NE()

        item_groups = {}

        for ne in set_NEs:
            rule_production = []

            rules_id = self.__conn.get_item_rules(ne)

            if rules_id is not None:
                for rule_id in rules_id:

                    prod = self.__conn.get_rule_production(rule_id)
                    prod.remove(ne.surface)
                    if len(prod) > 0:
                        rule_production.extend(prod)

                item_groups[ne] = sorted(rule_production)

        print('fim de group items')

        return item_groups


    def build_bin_groups(self, item_groups):

        all_items = []

        for ne in item_groups.keys():
            all_items.extend(item_groups[ne])

        all_items = all_items # todo <<<<<---- se inserimos um set neste ponto, nos evitamos as repeticoes. Temos que testar com e sem para verificar se isso muda o score de similaridade

        ml_group = {}
        seed_production = {}
        print('construindo binaire rules')
        gc.collect()
        result_seed = open('result_seed.txt', 'w+', encoding='utf8')
        result_pot_ne = open('result_pot_ne.txt', 'w+', encoding='utf-8')

        for ne in item_groups.keys():

            binaire_prod = []
            ne_prod = item_groups[ne]

            for item in sorted(all_items):

                if item in ne_prod:
                    ne_prod.remove(item)
                    binaire_prod.append(1)
                else:
                    binaire_prod.append(0)

            if ne.is_seed:
                result_seed.write(ne.surface + ' - ' + str(binaire_prod) + "\n")
                # seed_production[ne.surface] = binaire_prod
            else:
                # ml_group[ne.surface] = binaire_prod
                result_pot_ne.write(ne.surface + ' - ' + str(binaire_prod) + "\n")
            gc.collect()

        result_pot_ne.close()
        result_seed.close()
        gc.collect()


    def analyse_bin_groups(self):

        seeds = []
        with(open('result_seed.txt', 'r', encoding='utf-8')) as result_seed:

            for line2 in result_seed:

                bin_seed_prod = line2.split(' - ')[1].replace('[', '').replace(']', '').strip().split(',')

                seeds.append([item.strip() for item in bin_seed_prod])

                del line2
                del bin_seed_prod
                gc.collect()

        seed_total = len(seeds[0])

        similarities = {}
        count_ne = 1
        with(open('result_pot_ne.txt', 'r', encoding='utf-8')) as result_seed:

            # total_nes = len(result_seed.readlines())

            for line1 in result_seed:

                line_parts = line1.split(' - ')
                bin_pot_ne_prod = line_parts[1].replace('[', '').replace(']', '').strip().split(',')
                pot_ne = line_parts[0].strip()
                bin_pot_ne_prod = [item.strip() for item in bin_pot_ne_prod]

                del line_parts
                del line1
                gc.collect()

                total_diffs = []

                count_seed = 1

                for seed in seeds:

                    print(str(count_ne)  + ' - ' + str(count_seed))
                    count_seed += 1

                    count = 0
                    for index in range(0, len(bin_pot_ne_prod)):
                        if bin_pot_ne_prod[index] == seed[index]:
                            count += 1

                    total_diffs.append(count/seed_total)

                    del seed
                    gc.collect()
                count_ne += 1

                import numpy as np
                similarities[pot_ne] = np.mean(total_diffs)

        sorted_similarites = sorted(similarities.items(), key=operator.itemgetter(1))

        for item in sorted_similarites:

            print(item[0] + ' seed similarity - ' + str(item[1]))



