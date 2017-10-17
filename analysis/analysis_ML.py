from builtins import enumerate
from sklearn.feature_extraction.text import TfidfVectorizer
import operator
import regex
import gc
from database.mySQLConnector import MySQLConnector
from sklearn.neural_network import MLPClassifier
import pickle

class Analyze_NE(object):

    def __init__(self, connector, ids_train, qtd_ngrams, context, corpus, path_sw):
        assert isinstance(connector, MySQLConnector)
        self.corpus = corpus
        self.qtd_ngrams = qtd_ngrams
        self.context = context
        self.names_ids = {}
        self.conn = connector
        with open(path_sw, 'r', encoding='utf-8') as stop_w_file:
            self.stop_w_list = [stop_w.strip() for stop_w in stop_w_file.readlines()]
            # self.stop_w_list = []

        try:
            self.base_ngrams_L = pickle.load(open("/home/alexandre/PycharmProjects/Memoire/analysis/" + str(self.qtd_ngrams) + "_base_ngrams_L.pk", "rb"))
            self.base_ngrams_R = pickle.load(open("/home/alexandre/PycharmProjects/Memoire/analysis/" + str(self.qtd_ngrams) + "_base_ngrams_L.pk", "rb"))
            print("Ngrams base loaded from pickles")
        except FileNotFoundError:
            self.base_ngrams_L, self.base_ngrams_R = self.build_set_base(ids_train, True)
        self.clf = MLPClassifier(solver='lbfgs', learning_rate='adaptive')
        # self.clf = DecisionTreeClassifier()

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

    def get_individual_ngrams_rules(self, idpotential_ne, verbose=False):

        ## get rules from ontology
        pot_nes = self.conn.get_potential_ne_where("idpotential_ne", idpotential_ne)
        if verbose:
            print("id:" + str(pot_nes[0].idpotential_ne) + " - " +pot_nes[0].surface )

        self.names_ids[pot_nes[0].idpotential_ne]= pot_nes[0].surface

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

                    if not regex.match(r"(\.|\(|\)|,|;|:|-|<unknown>)", lemmas[-1]) and lemmas[
                        -1] not in self.stop_w_list \
                            and lemmas[-1] in ngram_pos_L[0].keys():
                        ngram_pos_L[0][lemmas[-1]] += 1
                    else:
                        ngram_pos_L[0][lemmas[-1]] = 1

                    if self.qtd_ngrams >= 2 or self.qtd_ngrams == -2:
                        if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)", lemmas[-2]) and lemmas[
                            -2] not in self.stop_w_list \
                                and lemmas[-2] in ngram_pos_L[1].keys():
                            ngram_pos_L[1][lemmas[-2]] += 1
                        else:
                            ngram_pos_L[1][lemmas[-2]] = 1

                    if self.qtd_ngrams >= 3 or self.qtd_ngrams == -3:
                        if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)", lemmas[-3]) and lemmas[
                            -3] not in self.stop_w_list \
                                and lemmas[-3] in ngram_pos_L[2].keys():
                            ngram_pos_L[2][lemmas[-3]] += 1
                        else:
                            ngram_pos_L[2][lemmas[-3]] = 1

                    if self.qtd_ngrams >= 4 or self.qtd_ngrams == -4:

                        if "<sep>".join(lemmas[-2:]) in ngram_pos_L[3].keys():
                            ngram_pos_L[3]["<sep>".join(lemmas[-2:])] += 1
                        else:
                            ngram_pos_L[3]["<sep>".join(lemmas[-2:])] = 1

                    if self.qtd_ngrams == 5  or self.qtd_ngrams == -5:

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

                    if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)", lemmas[0]) and lemmas[
                        0] not in self.stop_w_list \
                            and lemmas[0] in ngram_pos_R[0].keys():
                        ngram_pos_R[0][lemmas[0]] += 1
                    else:
                        ngram_pos_R[0][lemmas[0]] = 1

                    if self.qtd_ngrams >= 2  or self.qtd_ngrams == -2:

                        if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)", lemmas[1]) and lemmas[
                            1] not in self.stop_w_list \
                                and lemmas[1] in ngram_pos_R[1].keys():
                            ngram_pos_R[1][lemmas[1]] += 1
                        else:
                            ngram_pos_R[1][lemmas[1]] = 1

                    if self.qtd_ngrams >= 3  or self.qtd_ngrams == -3:

                        if not regex.match(r"(\.|\(|\)|,|;|:|-|\+|\[|\]|<unknown>)", lemmas[2]) and lemmas[
                            2] not in self.stop_w_list \
                                and lemmas[2] in ngram_pos_R[2].keys():
                            ngram_pos_R[2][lemmas[2]] += 1
                        else:
                            ngram_pos_R[2][lemmas[2]] = 1

                    if self.qtd_ngrams >= 4  or self.qtd_ngrams == -4:

                        if "<sep>".join(lemmas[:2]) in ngram_pos_R[3].keys():
                            ngram_pos_R[3]["<sep>".join(lemmas[:2])] += 1
                        else:
                            ngram_pos_R[3]["<sep>".join(lemmas[:2])] = 1

                    if self.qtd_ngrams == 5  or self.qtd_ngrams == -5:

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

    def build_set_base(self, ids, save_pickles=False):
        base_ngrams_R = {}
        base_ngrams_L = {}

        for id in ids:

            print("buildin base for id: ", str(id))

            pot_ngramsL, pot_ngramsR = self.get_individual_ngrams_rules(id)

            for index in sorted(pot_ngramsR.keys()):

                if index not in base_ngrams_R.keys():
                    base_ngrams_R[index] = sorted(list(pot_ngramsR[index].keys()))
                else:
                    temp_list = base_ngrams_R[index] + list(pot_ngramsR[index].keys())
                    base_ngrams_R[index] = sorted(list(set(temp_list)))

            for index in sorted(pot_ngramsL.keys()):

                if index not in base_ngrams_L.keys():
                    base_ngrams_L[index] = sorted(list(pot_ngramsL[index].keys()))
                else:
                    temp_list = base_ngrams_L[index] + list(pot_ngramsL[index].keys())
                    base_ngrams_L[index] = sorted(list(set(temp_list)))

        print("base ngrams done!")

        if save_pickles:

            pickle.dump(base_ngrams_L, open("/home/alexandre/PycharmProjects/Memoire/analysis/" + str(self.qtd_ngrams) + "_base_ngrams_L.pk", "wb"), protocol=pickle.HIGHEST_PROTOCOL)
            pickle.dump(base_ngrams_R, open("/home/alexandre/PycharmProjects/Memoire/analysis/" + str(self.qtd_ngrams) + "_base_ngrams_R.pk", "wb"), protocol=pickle.HIGHEST_PROTOCOL)
            print("pickles saved!")


        return base_ngrams_L, base_ngrams_R

    def build_set(self, ids):

        X = []

        score_ngrams_R = {}
        score_ngrams_L = {}

        id_scores = []

        for id in ids:

            pot_ngramsL, pot_ngramsR = self.get_individual_ngrams_rules(id)

            for index in sorted(self.base_ngrams_L.keys()):

                temp_scores = []

                for ngram in sorted(self.base_ngrams_L[index]):

                    if ngram in pot_ngramsL[index]:
                        temp_scores.append(pot_ngramsL[index][ngram])
                        # temp_scores.append(1)
                    else:
                        temp_scores.append(0)

                score_ngrams_L[index] = temp_scores

            for index in sorted(self.base_ngrams_R.keys()):

                temp_scores = []

                for ngram in sorted(self.base_ngrams_R[index]):

                    if ngram in pot_ngramsR[index]:
                        temp_scores.append(pot_ngramsR[index][ngram])
                        # temp_scores.append(1)
                    else:
                        temp_scores.append(0)

                score_ngrams_R[index] = temp_scores

            id_scores.append((score_ngrams_L, score_ngrams_R))

        for scores in id_scores:
            all_scores_L = []
            all_scores_R = []
            for key in scores[0].keys():
                all_scores_L.extend(scores[0][key])

            for key in scores[1].keys():
                all_scores_R.extend(scores[1][key])

            X.append(all_scores_L + all_scores_R)
        return X

    def build_X(self, all_ids, ids_set):

        feats_tfidf = []
        try:
            matrix = pickle.load(open("matrix_test_" + self.corpus + "_" + str(self.qtd_ngrams) + "_" + self.context + ".pk", "rb"))
        except FileNotFoundError:

            all_corpus_tf_idf = []

            for index1 in all_ids:
                print("building set id: ", str(index1))
                all_corpus_tf_idf.append(self.get_rules_tf_idf(index1, False))

            vectorizer = TfidfVectorizer()
            gc.collect()
            matrix = vectorizer.fit_transform(all_corpus_tf_idf)
            pickle.dump(matrix, open("matrix_test_" + self.corpus + "_" + str(self.qtd_ngrams) + "_" + self.context + ".pk", "wb"))

        for index1, i in enumerate(matrix):

            # add 1 to index to match with db index
            if index1 + 1 not in ids_set:
                continue

            feats = {}
            for index2, indice in enumerate(i.indices):
                feats[indice] = i.data[index2]
            feats_tfidf.append(feats)

        gc.collect()
        all_feats = []
        for feat in feats_tfidf:
            feats = []
            for indice in matrix.indices:
                if indice in feat.keys():
                    feats.append(feat[indice])
                else:
                    feats.append(0)
            all_feats.append(feats)
        return all_feats

    def fitting_model(self, X, y, verbose=False):

        if verbose:
            print("Training model")

        self.clf.fit(X, y)

    def predict_output(self, X_test):

        output = self.clf.predict(X_test)[0]
        return output

    def get_rules_tf_idf(self, idpotential_ne, verbose=False):

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

                    if self.qtd_ngrams >= 2 or self.qtd_ngrams == -2:
                        if lemmas[-2] in ngram_pos_L[1].keys():
                            ngram_pos_L[1][lemmas[-2]] += 1
                        else:
                            ngram_pos_L[1][lemmas[-2]] = 1

                    if self.qtd_ngrams >= 3  or self.qtd_ngrams == -3:
                        if lemmas[-3] in ngram_pos_L[2].keys():
                            ngram_pos_L[2][lemmas[-3]] += 1
                        else:
                            ngram_pos_L[2][lemmas[-3]] = 1

                    if self.qtd_ngrams >= 4  or self.qtd_ngrams == -4:

                        if "<sep>".join(lemmas[-2:]) in ngram_pos_L[3].keys():
                            ngram_pos_L[3]["<sep>".join(lemmas[-2:])] += 1
                        else:
                            ngram_pos_L[3]["<sep>".join(lemmas[-2:])] = 1

                    if self.qtd_ngrams == 5  or self.qtd_ngrams == -5:

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

                    if self.qtd_ngrams >= 2 or self.qtd_ngrams == -2:

                        if lemmas[1] in ngram_pos_R[1].keys():
                            ngram_pos_R[1][lemmas[1]] += 1
                        else:
                            ngram_pos_R[1][lemmas[1]] = 1

                    if self.qtd_ngrams >= 3  or self.qtd_ngrams == -3:

                        if lemmas[2] in ngram_pos_R[2].keys():
                            ngram_pos_R[2][lemmas[2]] += 1
                        else:
                            ngram_pos_R[2][lemmas[2]] = 1

                    if self.qtd_ngrams >= 4  or self.qtd_ngrams == -4:

                        if "<sep>".join(lemmas[:2]) in ngram_pos_R[3].keys():
                            ngram_pos_R[3]["<sep>".join(lemmas[:2])] += 1
                        else:
                            ngram_pos_R[3]["<sep>".join(lemmas[:2])] = 1

                    if self.qtd_ngrams == 5  or self.qtd_ngrams == -5:

                        if "<sep>".join(lemmas[:3]) in ngram_pos_R[4].keys():
                            ngram_pos_R[4]["<sep>".join(lemmas[:3])] += 1
                        else:
                            ngram_pos_R[4]["<sep>".join(lemmas[:3])] = 1

                except IndexError:
                    pass
        indiv_corpus_tfidf_L = ""
        indiv_corpus_tfidf_R = ""

        if self.context == 'both' or self.context=='L':
            words_L = []
            for key in ngram_pos_L.keys():

                if key > self.qtd_ngrams:
                    break
                ordered_grams = sorted(ngram_pos_L[key].items(), key=operator.itemgetter(1))
                for item in ordered_grams:
                    words_L.append("L_" + str(key) +"_"+ str(item[0]))
            indiv_corpus_tfidf_L = " ".join(words_L)

        if self.context == 'both' or self.context == 'R':
            words_R = []
            for key in ngram_pos_R.keys():

                if key > self.qtd_ngrams:
                    break

                ordered_grams = sorted(ngram_pos_R[key].items(), key=operator.itemgetter(1))
                for item in ordered_grams:
                    words_R.append("R_" + str(key) +"_"+ str(item[0]))

            indiv_corpus_tfidf_R = " ".join(words_R)

        return indiv_corpus_tfidf_L + indiv_corpus_tfidf_R
