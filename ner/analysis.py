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
        :return: 2 bi dimensional arrays containing the dictionaries of lemmas
        """

        all_rules = self.conn.get_rule_ontonlogy()

        dic_rules_L = []
        dic_rules_R = []

        for line in all_rules:

            lemas = line[8].split("<sep>")
            tags = line[11].split("<sep>")

            if line[2] == 'R':

                count_pos = 0
                for index, lema in enumerate(lemas):

                    key_dic = tags[index] + "<sep>" + lema

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
                lemas = reversed(lemas)
                tags = list(reversed(tags))

                count_pos = 0
                for index, lema in enumerate(lemas):

                    key_dic = tags[index] + "<sep>" + lema

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


dic_rules_L = {}
connector = MySQLConnector('memoire', '20060907jl', 'root')
ana = Analyze_NE(connector)

dic_rules_L[4] = ana.get_grams('L', 4)
dic_rules_L[3] = ana.get_grams('L', 3)
dic_rules_L[2] = ana.get_grams('L', 2)
dic_rules_L[1] = ana.get_grams('L', 1)


for key in dic_rules_L[1].keys():

    if key.startswith('V'):
        print(key + ' ' + str(dic_rules_L[1][key]))

print('o')