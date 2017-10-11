import operator
import regex
import sys
from sklearn.feature_extraction.text import TfidfVectorizer
from database.mySQLConnector import MySQLConnector
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn import tree
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.svm import SVC
import pickle

class AnalyzeRules(object):

    def __init__(self, conn):

        self.conn = conn
        with open('../ner/100_stop_words.txt', 'r', encoding='utf-8') as stop_w_file:
            self.stop_w_list = [stop_w.strip() for stop_w in stop_w_file.readlines()]

    def building_percentage(self, ngrams):

        total = 0
        for key in ngrams:
            total += ngrams[key]

        for key in ngrams:
            ngrams[key] = ngrams[key] / total

        return ngrams

    def get_individual_ngrams_rules(self, idpotential_ne, qtd_ngrams, verbose=False):

        ## get rules from ontology
        pot_nes = self.conn.get_potential_ne_where("idpotential_ne", idpotential_ne)
        if verbose:
            print(str(pot_nes[0].idpotential_ne) + "," +pot_nes[0].surface )

        ngram_pos_L = {}
        ngram_pos_L[0] = {}
        ngram_pos_L[1] = {}
        ngram_pos_L[2] = {}
        ngram_pos_L[3] = {}
        ngram_pos_L[4] = {}

        ngram_pos_R = {}
        ngram_pos_R[0] = {}
        ngram_pos_R[1] = {}
        ngram_pos_R[2] = {}
        ngram_pos_R[3] = {}
        ngram_pos_R[4] = {}

        for pot_ne in pot_nes:
            # print("## - " + pot_ne.surface + " id: " + str(pot_ne.idpotential_ne))
            rules = self.conn.get_rules_by_pot_ne_id(pot_ne.idpotential_ne)

            # build rules score
            for rule in rules:

                lemmas = rule.lemmas.split("<sep>")
                rule_pos = rule.POS.split("<sep>")

                try:
                    # get ngrams 1st position
                    if rule.orientation != 'L':
                        continue

                    if not regex.match(r"(\.|\(|\)|,|;|:|-|<unknown>)",lemmas[-1]) and lemmas[-1] not in self.stop_w_list\
                            and lemmas[-1] in ngram_pos_L[0].keys():
                        ngram_pos_L[0][lemmas[-1]] += 1
                    else:
                        ngram_pos_L[0][lemmas[-1]] = 1

                    if qtd_ngrams >= 2:
                        if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)",lemmas[-2]) and lemmas[-2] not in self.stop_w_list \
                                and lemmas[-2] in ngram_pos_L[1].keys():
                            ngram_pos_L[1][lemmas[-2]] += 1
                        else:
                            ngram_pos_L[1][lemmas[-2]] = 1

                        if qtd_ngrams >= 3:
                            if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)",lemmas[-3]) and lemmas[-3] not in self.stop_w_list \
                                    and lemmas[-3] in ngram_pos_L[2].keys():
                                ngram_pos_L[2][lemmas[-3]] += 1
                            else:
                                ngram_pos_L[2][lemmas[-3]] = 1

                            if qtd_ngrams >= 4:

                                if "<sep>".join(lemmas[-2:]) in ngram_pos_L[3].keys():
                                    ngram_pos_L[3]["<sep>".join(lemmas[-2:])] += 1
                                else:
                                    ngram_pos_L[3]["<sep>".join(lemmas[-2:])] = 1

                                if qtd_ngrams == 5:

                                    if "<sep>".join(lemmas[-3:]) in ngram_pos_L[4].keys():
                                        ngram_pos_L[4]["<sep>".join(lemmas[-3:])] += 1
                                    else:
                                        ngram_pos_L[4]["<sep>".join(lemmas[-3:])] = 1

                except IndexError:
                    pass

            # build rules score
            for rule in rules:

                lemmas = rule.lemmas.split("<sep>")


                try:
                    # get ngrams 1st position
                    if rule.orientation != 'R':
                        continue

                    if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)",lemmas[0]) and lemmas[0] not in self.stop_w_list \
                            and lemmas[0] in ngram_pos_R[0].keys():
                        ngram_pos_R[0][lemmas[0]] += 1
                    else:
                        ngram_pos_R[0][lemmas[0]] = 1

                    if qtd_ngrams >= 2:

                        if  not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)",lemmas[1])  and lemmas[1] not in self.stop_w_list \
                                and lemmas[1] in ngram_pos_R[1].keys():
                            ngram_pos_R[1][lemmas[1]] += 1
                        else:
                            ngram_pos_R[1][lemmas[1]] = 1

                        if qtd_ngrams >= 3:

                            if  not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)",lemmas[2]) and lemmas[2] not in self.stop_w_list \
                                    and lemmas[2] in ngram_pos_R[2].keys():
                                ngram_pos_R[2][lemmas[2]] += 1
                            else:
                                ngram_pos_R[2][lemmas[2]] = 1

                            if qtd_ngrams >= 4:

                                if "<sep>".join(lemmas[:2]) in ngram_pos_R[3].keys():
                                    ngram_pos_R[3]["<sep>".join(lemmas[:2])] += 1
                                else:
                                    ngram_pos_R[3]["<sep>".join(lemmas[:2])] = 1

                                if qtd_ngrams == 5:

                                    if "<sep>".join(lemmas[:3]) in ngram_pos_R[4].keys():
                                        ngram_pos_R[4]["<sep>".join(lemmas[:3])] += 1
                                    else:
                                        ngram_pos_R[4]["<sep>".join(lemmas[:3])] = 1

                except IndexError:
                    pass

        ngram_pos_L[0] = self.building_percentage(ngram_pos_L[0])
        ngram_pos_L[1] = self.building_percentage(ngram_pos_L[1])
        ngram_pos_L[2] = self.building_percentage(ngram_pos_L[2])
        ngram_pos_L[3] = self.building_percentage(ngram_pos_L[3])
        ngram_pos_L[4] = self.building_percentage(ngram_pos_L[4])

        ngram_pos_R[0] = self.building_percentage(ngram_pos_R[0])
        ngram_pos_R[1] = self.building_percentage(ngram_pos_R[1])
        ngram_pos_R[2] = self.building_percentage(ngram_pos_R[2])
        ngram_pos_R[3] = self.building_percentage(ngram_pos_R[3])
        ngram_pos_R[4] = self.building_percentage(ngram_pos_R[4])

        for key in ngram_pos_L.keys():

            ordered_grams = sorted(ngram_pos_L[key].items(), key=operator.itemgetter(1))
            for item in ordered_grams:
                print(str(pot_nes[0].idpotential_ne) + "," +pot_nes[0].surface +" ," +  str(key) + "," + "L," +str(item[0]) + "," + str(item[1]))

        for key in ngram_pos_R.keys():

            ordered_grams = sorted(ngram_pos_R[key].items(), key=operator.itemgetter(1))
            for item in ordered_grams:
                print(str(pot_nes[0].idpotential_ne) + "," +pot_nes[0].surface +" ," + str(key) + "," + "R," + str(item[0]) + "," + str(item[1]))




        return ngram_pos_L, ngram_pos_R

    def get_rules_tf_idf(self, idpotential_ne, qtd_ngrams, verbose=False):

        ## get rules from ontology
        pot_nes = self.conn.get_potential_ne_where("idpotential_ne", idpotential_ne)
        if verbose:
            print(str(pot_nes[0].idpotential_ne) + "," + pot_nes[0].surface)

        ngram_pos_L = {}
        ngram_pos_L[0] = {}
        ngram_pos_L[1] = {}
        ngram_pos_L[2] = {}
        ngram_pos_L[3] = {}
        ngram_pos_L[4] = {}

        ngram_pos_R = {}
        ngram_pos_R[0] = {}
        ngram_pos_R[1] = {}
        ngram_pos_R[2] = {}
        ngram_pos_R[3] = {}
        ngram_pos_R[4] = {}

        for pot_ne in pot_nes:
            # print("## - " + pot_ne.surface + " id: " + str(pot_ne.idpotential_ne))
            rules = self.conn.get_rules_by_pot_ne_id(pot_ne.idpotential_ne)

            # build rules score
            for rule in rules:

                lemmas = rule.lemmas.split("<sep>")
                rule_pos = rule.POS.split("<sep>")

                try:
                    # get ngrams 1st position
                    if rule.orientation != 'L':
                        continue

                    if lemmas[-1] in ngram_pos_L[0].keys():
                        ngram_pos_L[0][lemmas[-1]] += 1
                    else:
                        ngram_pos_L[0][lemmas[-1]] = 1

                    if qtd_ngrams >= 2:
                        if lemmas[-2] in ngram_pos_L[1].keys():
                            ngram_pos_L[1][lemmas[-2]] += 1
                        else:
                            ngram_pos_L[1][lemmas[-2]] = 1

                        if qtd_ngrams >= 3:
                            if lemmas[-3] in ngram_pos_L[2].keys():
                                ngram_pos_L[2][lemmas[-3]] += 1
                            else:
                                ngram_pos_L[2][lemmas[-3]] = 1

                            if qtd_ngrams >= 4:

                                if "<sep>".join(lemmas[-2:]) in ngram_pos_L[3].keys():
                                    ngram_pos_L[3]["<sep>".join(lemmas[-2:])] += 1
                                else:
                                    ngram_pos_L[3]["<sep>".join(lemmas[-2:])] = 1

                                if qtd_ngrams == 5:

                                    if "<sep>".join(lemmas[-3:]) in ngram_pos_L[4].keys():
                                        ngram_pos_L[4]["<sep>".join(lemmas[-3:])] += 1
                                    else:
                                        ngram_pos_L[4]["<sep>".join(lemmas[-3:])] = 1

                except IndexError:
                    pass

            # build rules score
            for rule in rules:

                lemmas = rule.lemmas.split("<sep>")

                try:
                    # get ngrams 1st position
                    if rule.orientation != 'R':
                        continue

                    if lemmas[0] in ngram_pos_R[0].keys():
                        ngram_pos_R[0][lemmas[0]] += 1
                    else:
                        ngram_pos_R[0][lemmas[0]] = 1

                    if qtd_ngrams >= 2:

                        if lemmas[1] in ngram_pos_R[1].keys():
                            ngram_pos_R[1][lemmas[1]] += 1
                        else:
                            ngram_pos_R[1][lemmas[1]] = 1

                        if qtd_ngrams >= 3:

                            if lemmas[2] in ngram_pos_R[2].keys():
                                ngram_pos_R[2][lemmas[2]] += 1
                            else:
                                ngram_pos_R[2][lemmas[2]] = 1

                            if qtd_ngrams >= 4:

                                if "<sep>".join(lemmas[:2]) in ngram_pos_R[3].keys():
                                    ngram_pos_R[3]["<sep>".join(lemmas[:2])] += 1
                                else:
                                    ngram_pos_R[3]["<sep>".join(lemmas[:2])] = 1

                                if qtd_ngrams == 5:

                                    if "<sep>".join(lemmas[:3]) in ngram_pos_R[4].keys():
                                        ngram_pos_R[4]["<sep>".join(lemmas[:3])] += 1
                                    else:
                                        ngram_pos_R[4]["<sep>".join(lemmas[:3])] = 1

                except IndexError:
                    pass

        indiv_corpus_tfidf = {}
        words_L = []
        for key in ngram_pos_L.keys():
            words_ngram = []
            ordered_grams = sorted(ngram_pos_L[key].items(), key=operator.itemgetter(1))
            for item in ordered_grams:
                words_ngram.append(str(item[0]))
            words_L.append(" ".join(words_ngram))

        words_R = []
        for key in ngram_pos_R.keys():
            words_ngram = []

            ordered_grams = sorted(ngram_pos_R[key].items(), key=operator.itemgetter(1))
            for item in ordered_grams:
                words_ngram.append(str(item[0]))
            words_R.append(" ".join(words_ngram))

        indiv_corpus_tfidf["L"] = words_L
        indiv_corpus_tfidf["R"] = words_R

        return indiv_corpus_tfidf

connector = MySQLConnector('memoire_fut', '20060907jl', 'root', host='localhost')

ids_train = range(1,10)
analyzer = AnalyzeRules(connector)

# set_ngrams = []
ngrams_R = []
ngrams_L = []


all_corpus_tf_idf = []

for index1 in range(1, 10):
    all_corpus_tf_idf.append(analyzer.get_rules_tf_idf(index1, 3, False)["L"][0])

vectorizer = TfidfVectorizer()
matrix = vectorizer.fit_transform(all_corpus_tf_idf)
for i in matrix:
    feats = {}
    for index, indice in enumerate(i.indices):
        feats[indice] = i.data[index]

# print(matrix)
# todo atençao inserir +1 no index do array tf idf para ficar equivalente ao index da base de dados
# for i, feature in enumerate(vectorizer.get_feature_names()):
#     print(i, feature)