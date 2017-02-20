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

        all_rules = self.conn.get_all_rules()

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
        pass


c = Analyze_NE()
r, l = c.get_rules_dicts()

# pickle._dump(r, open("rules_r.pk", "wb"))
# pickle._dump(l, open("rules_l.pk", "wb"))

l_gold = pickle.load(open("rules_l.pk", "rb"))
r_gold = pickle.load(open("rules_r.pk", "rb"))

for index, pos in enumerate(l):

    for key in pos.keys():

        dist = 0

        for dics in l_gold:

            if key in dics.keys():
                print(key + " - "+ str(dist)+ " score: " + str(dics[key]) + " key position " + str(index))
            dist += 1
print("---------------")

for index, pos in enumerate(r):

    for key in pos.keys():

        dist = 0

        for dics in r_gold:

            if key in dics.keys():
                print(key + " - "+ str(dist)+ " score: " + str(dics[key]) + " key position " + str(index))
            dist += 1

print('o')