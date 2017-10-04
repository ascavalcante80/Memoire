import operator

from database.mySQLConnector import MySQLConnector
import pickle

class Analyze_NE(object):

    def __init__(self, connector):
        assert isinstance(connector, MySQLConnector)
        self.conn = connector

    def get_ngrams_rules_seed_ontolgy(self):

        ## get rules from ontology
        pot_nes = self.conn.get_potential_ne_where("type", "O")
        pot_nes.extend(self.conn.get_potential_ne_where("type", "S"))

        ngram_pos = {}
        ngram_pos[0] = {}
        ngram_pos[1] = {}
        ngram_pos[2] = {}

        for pot_ne in pot_nes:
            rules = self.conn.get_rules_by_pot_ne_id(pot_ne.idpotential_ne)

            # build rules score
            for rule in rules:

                lemmas = rule.lemmas.split("<sep>")

                try:
                    # get ngrams 1st position
                    if rule.orientation == 'L':
                        lemma = lemmas[-1]
                    else:
                        continue

                    if lemma in ngram_pos[0].keys():
                        ngram_pos[0][lemma] += 1
                    else:
                        ngram_pos[0][lemma] = 1

                    # get ngrams 2nd position
                    if rule.orientation == 'L':
                        lemma = lemmas[-2]
                    else:
                        lemma = lemmas[1]

                    if lemma in ngram_pos[1].keys():
                        ngram_pos[1][lemma] += 1
                    else:
                        ngram_pos[1][lemma] = 1

                    # get ngrams 3rd position
                    if rule.orientation == 'L':
                        lemma = lemmas[-3]
                    else:
                        lemma = lemmas[2]

                    if lemma in ngram_pos[2].keys():
                        ngram_pos[2][lemma] += 1
                    else:
                        ngram_pos[2][lemma] = 1

                except IndexError:
                    pass

        ngram_pos[0] = self.building_percentage(ngram_pos[0])
        ngram_pos[1] = self.building_percentage(ngram_pos[1])
        ngram_pos[2] = self.building_percentage(ngram_pos[2])

        return ngram_pos

    def building_percentage(self, ngrams):

        total = 0
        for key in ngrams:
            total += ngrams[key]

        for key in ngrams:
            ngrams[key] = ngrams[key] / total

        return ngrams

    def get_individual_ngrams_rules(self, idpotential_ne):

        ## get rules from ontology
        pot_nes = self.conn.get_potential_ne_where("idpotential_ne", idpotential_ne)

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

                try:
                    # get ngrams 1st position
                    if rule.orientation != 'L':
                        continue

                    if lemmas[-1] in ngram_pos_L[0].keys():
                        ngram_pos_L[0][lemmas[-1]] += 1
                    else:
                        ngram_pos_L[0][lemmas[-1]] = 1

                    if lemmas[-2] in ngram_pos_L[1].keys():
                        ngram_pos_L[1][lemmas[-2]] += 1
                    else:
                        ngram_pos_L[1][lemmas[-2]] = 1

                    if lemmas[-3] in ngram_pos_L[2].keys():
                        ngram_pos_L[2][lemmas[-3]] += 1
                    else:
                        ngram_pos_L[2][lemmas[-3]] = 1

                    if "<sep>".join(lemmas[-2:]) in ngram_pos_L[3].keys():
                        ngram_pos_L[3]["<sep>".join(lemmas[-2:])] += 1
                    else:
                        ngram_pos_L[3]["<sep>".join(lemmas[-2:])] = 1

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

                    if lemmas[1] in ngram_pos_R[1].keys():
                        ngram_pos_R[1][lemmas[1]] += 1
                    else:
                        ngram_pos_R[1][lemmas[1]] = 1

                    if lemmas[2] in ngram_pos_R[2].keys():
                        ngram_pos_R[2][lemmas[2]] += 1
                    else:
                        ngram_pos_R[2][lemmas[2]] = 1

                    if "<sep>".join(lemmas[:2]) in ngram_pos_R[3].keys():
                        ngram_pos_R[3]["<sep>".join(lemmas[:2])] += 1
                    else:
                        ngram_pos_R[3]["<sep>".join(lemmas[:2])] = 1

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

        return ngram_pos_L, ngram_pos_R


connector = MySQLConnector('memoire_fut', '20060907jl', 'root', host='localhost')

# counting rules of NE
movies_count= {}
for index1 in range(1, 3429):

    name, count = connector.get_ne_count(str(index1))
    movies_count[name] = count

ordered_scores = sorted(movies_count.items(), key=operator.itemgetter(1))
for i in ordered_scores:
    print(i)


analyzer = Analyze_NE(connector)

ids = [211, 384, 53, 1084, 511, 292, 41,485, 496, 1525, 1587, 3376,
       59, 198, 60, 64,174, 193,225,264,290, 321,314,355,433]
y = [1, 1, 1, 1, 1, 1, 1, 1,1, 1, 1, 1, 0, 0, 0,0,0,0,0,0,0,0,0,0,0]

ngrams_R = []
ngrams_L = []
set_ngrams = []

for id in ids:

    pot_ngramsL, pot_ngramsR = analyzer.get_individual_ngrams_rules(id)

    for index in pot_ngramsR:
        ngrams_R.extend(pot_ngramsR[index].keys())

    for index in pot_ngramsL:
        ngrams_L.extend(pot_ngramsL[index].keys())

    set_ngrams.append((pot_ngramsL, pot_ngramsR))

ngrams_R = set(ngrams_R)
ngrams_L = set(ngrams_L)
X = []

for ngrams in set_ngrams:

    score_grams = []
    for ngram in ngrams_L:

        for key1 in ngrams[0].keys():

            if ngram not in ngrams[0][key1].keys():
                score_grams.append(0)
            else:
                score_grams.append(ngrams[0][key1][ngram])

    for ngram in ngrams_R:

        for key1 in ngrams[1].keys():

            if ngram not in ngrams[1][key1].keys():
                score_grams.append(1)
            else:
                score_grams.append(ngrams[1][key1][ngram])
    X.append(score_grams)












from sklearn.cross_validation import train_test_split
from sklearn.neural_network import MLPClassifier


X_train, X_test, y_train, y_test =train_test_split(X, y, random_state=4)
clf = MLPClassifier([2], activation='logistic', random_state=4)

clf.fit(X_train, y_train)

print("layers: %s. Number of outputs: %s" % (clf.n_layers_, clf.n_outputs_))

predictions = clf.predict(X_test)

print("precisão: ", str(clf.score(X_test, y_test)))

for i, p in enumerate(predictions):
    print("true: %s, Predicted: %s" % (y_test[i], p))



