import operator

from database.mySQLConnector import MySQLConnector
import pickle

class Analyze_NE(object):

    def __init__(self):
        self.conn = MySQLConnector()


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



c= MySQLConnector()
all_rules = c.get_all_rules_ontology()
dic_rules_count = {}
for rule in all_rules:

    if rule.orientation == 'L':
        lemmas = rule.lemmas.split('<sep>')[-4:]

        if len(lemmas) > 3:

            if "<sep>".join(lemmas) in dic_rules_count.keys():
                dic_rules_count["<sep>".join(lemmas)] += 1
            else:
                dic_rules_count["<sep>".join(lemmas)] = 1





print('o')