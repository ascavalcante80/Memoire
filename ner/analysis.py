import operator

from database.mySQLConnector import MySQLConnector
import pickle

class Analyze_NE(object):

    def __init__(self, connector):
        assert isinstance(connector, MySQLConnector)
        self.conn = connector

    def get_rules_dicts(self):
        """
        This function analyzes all the rules and builds dictionaries for each type of rules (left, right) containing
        the frequency of each lemma found in that position of the ngram.
        Ex.:
        positions

           3,2,1,0 - POTENTIAL_NE - 0, 1,2, 3.
            left                      right

        :return: 2 bi dimensional arrays containing the dictionaries of lemmas
        """

        all_rules = self.conn.get_rules_by_ne_type('S')
        all_rules.extend(self.conn.get_rules_by_ne_type('O'))

        dic_rules_L = []
        dic_rules_R = []

        for rule in all_rules:

            if rule.orientation == 'R':

                count_pos = 0
                for index, lemma in enumerate(rule.lemmas.split('<sep>')):

                    key_dic = rule.POS.split('<sep>')[index] + "<sep>" + lemma

                    try:
                        dic = dic_rules_R[count_pos]
                    except IndexError:
                        dic_rules_R.insert(count_pos,{})
                        dic = dic_rules_R[count_pos]


                    if key_dic in dic.keys():
                        dic[key_dic] += 1
                    else:
                        dic[key_dic] = 1

                    dic_rules_R[count_pos] = dic

                    count_pos +=1

            else:
                lemmas = reversed(rule.lemmas.split('<sep>'))
                POS = list(reversed(rule.POS.split('<sep>')))

                count_pos = 0
                for index, lemma in enumerate(lemmas):

                    key_dic = POS[index] + "<sep>" + lemma

                    try:
                        dic = dic_rules_L[count_pos]
                    except IndexError:
                        dic_rules_L.insert(count_pos,{})
                        dic = dic_rules_L[count_pos]


                    if key_dic in dic.keys():
                        dic[key_dic] += 1
                    else:
                        dic[key_dic] = 1

                    dic_rules_L[count_pos] = dic

                    count_pos +=1

        return dic_rules_R, dic_rules_L

    def compare_rules(self):

        r_test, l_test = self.get_rules_dicts(False)

        # getting rules from seeds
        r_gold, l_gold = self.get_rules_dicts(True)

        for index, pos in enumerate(l_test):

            for key in pos.keys():

                dist = 0

                for dics in l_gold:

                    if key in dics.keys():
                        print(key + " - " + str(dist) + " score: " + str(dics[key]) + " key position " + str(index))
                    dist += 1

        print("---------------")

        for index, pos in enumerate(r_test):

            for key in pos.keys():

                dist = 0

                for dics in r_gold:

                    if key in dics.keys():
                        print(key + " - " + str(dist) + " score: " + str(dics[key]) + " key position " + str(index))
                    dist += 1

    def get_grams(self, orientation, ngram):

        all_rules = self.conn.get_rules_by_ne_type('S')
        dic_rules = {}

        for rule in all_rules:

            if rule.orientation == orientation:

                if orientation == 'L':

                    lemmas = rule.lemmas.split('<sep>')[-ngram:]
                    pos = rule.POS.split('<sep>')[-ngram:]
                else:
                    lemmas = rule.lemmas.split('<sep>')[ngram:]
                    pos = rule.pos.split('<sep>')[ngram:]

                if len(lemmas) == ngram and ngram == 1:

                    if lemmas[0] != '<unknown>' and pos[0] != '':

                        if str(pos[0] + "<sep>" + lemmas[0]) in dic_rules.keys():
                            dic_rules[pos[0] + "<sep>" + lemmas[0]] += 1
                        else:
                            dic_rules[pos[0] + "<sep>" + lemmas[0]] = 1

                elif len(lemmas) == ngram and ngram != 1:

                    if "<sep>".join(lemmas) in dic_rules.keys():
                        dic_rules["<sep>".join(lemmas)] += 1
                    else:
                        dic_rules["<sep>".join(lemmas)] = 1
        return dic_rules

    def get_scores_ngrams(self, set_rules_child, ngram):

        ngram_limit = ngram
        all_rules = self.conn.get_rules_by_ne_type('S')
        all_rules.extend(self.conn.get_rules_by_ne_type('O'))
        ngram_score = {}

        ngram_score['total_L'] = 0
        ngram_score['total_R'] = 0

        ngram_score['total_L_integral'] = 0
        ngram_score['total_R_integral'] = 0

        for rule_child in set_rules_child:

            # count integral matchs
            integral_matchs = self.conn.get_potential_ne_rules(['rules_idrules'], [rule_child.idrules])
            ngram_score['total_' + rule_child.orientation + '_integral'] += len(integral_matchs)

            for rule in all_rules:

                if rule.idrules == rule_child.idrules:
                    continue

                if rule.orientation == rule_child.orientation:

                    lemmas_rule_db = str(rule.lemmas.split('<sep>'))
                    lemmas_child = rule_child.lemmas.split('<sep>')

                    if rule.orientation == 'R':

                        while(ngram_limit > 0):

                            if str(lemmas_child[:ngram_limit]) in str(lemmas_rule_db):

                                if str(ngram_limit) + '_' + rule.orientation in ngram_score.keys():
                                    dic_right = ngram_score[str(ngram_limit) + '_' + rule.orientation]
                                else:
                                    dic_right = {}
                                    ngram_score[str(ngram_limit) + '_' + rule.orientation] = dic_right

                                if str(lemmas_child[:ngram_limit]) in dic_right.keys():
                                    dic_right[str(lemmas_child[:ngram_limit])] += 1
                                else:
                                    dic_right[str(lemmas_child[:ngram_limit])] = 1

                                break

                            ngram_limit -= 1
                    else:
                        while(ngram_limit > 0):

                            if str(lemmas_child[-ngram_limit:]) in str(lemmas_rule_db):

                                if str(ngram_limit) + '_' + rule.orientation in ngram_score.keys():
                                    dic_left = ngram_score[str(ngram_limit) + '_' + rule.orientation]
                                else:
                                    dic_left = {}
                                    ngram_score[str(ngram_limit) + '_' + rule.orientation] = dic_left

                                if str(lemmas_child[-ngram_limit:]) in dic_left.keys():
                                    dic_left[str(lemmas_child[-ngram_limit:])] += 1
                                else:
                                    dic_left[str(lemmas_child[-ngram_limit:])] = 1


                                break

                            ngram_limit -= 1
                ngram_limit = ngram


        for rule in all_rules:

            ngram_score['total_' + rule.orientation] += 1

        return ngram_score

    def get_socres_POS(self, ngram):

        ngram_limit = ngram
        all_rules = self.conn.get_rules_by_ne_type('S')
        all_rules.extend(self.conn.get_rules_by_ne_type('O'))
        ngram_score = {}

        ngram_score['L'] = []
        ngram_score['R'] = []

        for rule in all_rules:

            if rule.orientation == 'L':

                try:

                    dict_POS = ngram_score[rule.orientation]

                except KeyError:
                    dict_POS = {}
                    ngram_score[rule.orientation] = dict_POS

                while (ngram_limit >= 0):

                    try:

                        POS_freqs = dict_POS[rule.POS[0]]

                    except KeyError:
                        POS_freqs = {}
                        dict_POS[rule.POS[0]] = {}

                    lemma = rule.lemmas.split("<sep>")[ngram_limit]

                    if lemma in POS_freqs.key():
                        POS_freqs[lemma] += 1
                    else:
                        POS_freqs[lemma] = 1

                    ngram_limit -= 1

                ngram_score[rule.orientation]

        return ngram_score

dic_rules_L = {}
connector = MySQLConnector('memoire', '20060907jl', 'root')
ana = Analyze_NE(connector)


set_rules_child = connector.get_rules_by_pot_ne_id(2)

score_ngrams = ana.get_scores_ngrams(set_rules_child, 3)

ana.get_rules_dicts()

dic_rules_L[4] = ana.get_grams('L', 4)
dic_rules_L[3] = ana.get_grams('L', 3)
dic_rules_L[2] = ana.get_grams('L', 2)
dic_rules_L[1] = ana.get_grams('L', 1)


for key in dic_rules_L[1].keys():

    if key.startswith('V'):
        print(key + ' ' + str(dic_rules_L[1][key]))

print('o')