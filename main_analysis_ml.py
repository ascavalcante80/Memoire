from builtins import enumerate
import operator
import gc
from database.mySQLConnector import MySQLConnector
from analysis.analysis_ML import Analyze_NE

connector = MySQLConnector('memoire', '20060907jl', 'root', host='localhost')

side = "R"
ngrams = 3
type_corpus = "filme"
last_id = 3486
ids_train = range(1, last_id)
analyzer = Analyze_NE(connector, ids_train, ngrams, side, type_corpus, "/home/alexandre/PycharmProjects/Memoire/ner/100_stop_words.txt")

# corpus cinema
ids = [1, 211, 384, 53, 1084, 511, 292, 41, 485, 3376,
       59, 198, 60, 64, 174, 193, 225, 264, 290, 321, 314, 355, 433, 671]
y = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

# set_ngrams = []
ngrams_R = []
ngrams_L = []

X = analyzer.build_X(range(1, last_id), ids)
analyzer.fitting_model(X, y)

potential_ne_counts = {}

for index1 in range(1, last_id):
    name, count = connector.get_ne_count(str(index1))
    potential_ne_counts[str(index1) + "<sep>" + name] = count

ordered_pot_nes = sorted(potential_ne_counts.items(), key=operator.itemgetter(1))

counter = 0
threshold = 200

with(open("./output_results/" + type_corpus + "_output_" + side + "_" + str(ngrams) + ".txt", "a")) as file_out:
    for index, i in enumerate(reversed(ordered_pot_nes)):

        id, name = i[0].split("<sep>")

        id = int(id)
        if int(id) in ids:
            continue
        X_train = analyzer.build_X(range(1, last_id), [id])

        output = analyzer.predict_output(X_train)

        print(str(index) + "/" + str(last_id) + ":  " + str(id) + " - " + name + " , " + str(output))
        file_out.write(str(index) + ":  " + str(id) + " - " + name + " , " + str(output) + "\n")

        if counter < threshold:
            X = X + X_train
            y.append(output)
            analyzer.fitting_model(X, y)
            counter += 1

        X_train = None
        gc.collect()