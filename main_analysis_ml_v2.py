from builtins import enumerate
import operator
import gc
from database.mySQLConnector import MySQLConnector
from analysis.analysis_ML import Analyze_NE

connector = MySQLConnector('memoire_fut', '20060907jl', 'root', host='localhost')

def launch_sys(side, ngrams, type_corpus, last_id, ids, y, bloc_ids=[]):


	ids_train = range(1, last_id)
	analyzer = Analyze_NE(connector, ids_train, ngrams, side, type_corpus, "/home/alexandre/PycharmProjects/Memoire/ner/100_stop_words.txt")

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
	threshold = 500

	with(open("./output_results/" + type_corpus + "_output_" + side + "_" + str(ngrams) + ".csv", "a")) as file_out:
	    for index, i in enumerate(reversed(ordered_pot_nes)):

	        id, name = i[0].split("<sep>")

	        id = int(id)
	        if int(id) in ids or int(id) in bloc_ids:
	            continue
	        X_train = analyzer.build_X(range(1, last_id), [id])

	        output = analyzer.predict_output(X_train)

	        print(str(index) + "/" + str(last_id) + ":  " + str(id) + " - " + name + " , " + str(output))
	        file_out.write(str(id) + "," + name + " , " + str(output) + "\n")

	        if counter < threshold:
	            X = X + X_train
	            y.append(output)
	            analyzer.fitting_model(X, y)
	            counter += 1

	        X_train = None
	        gc.collect()



blocs_ids = [16,23,58,64,65,72,85,98,104,105,117,118, 121,122,132,137,174,193,194,197,
204,206,209,210,214,216,218,221,226,229,232,266,313,318,324,329,343,355,381,510,
530,574,575,587,608,643,660,672,705,746,748,765,784, 814, 818,856,880,881,882,884,887,888,
890,891,893,894,895,902,908,909,910,911,912,913,935,936,947,953,991,1065,1076,1089]

ids = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,20,22,28,29,40,56,62,69,99, 113, 107,152,36, 207, 126]
y = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]


# corpus cinema
# ids = [1, 211, 384, 53, 1084, 511, 292, 41, 485, 3376,
#        59, 198, 60, 64, 174, 193, 225, 264, 290, 321, 314, 355, 433, 671]
# y = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


#print("------------------------------ R 2 PROBLEMA ---------------------------------------------------------")

#launch_sys("R", 2, "futebol", 1090, ids, y, blocs_ids)

print("------------------------------ R 3 ---------------------------------------------------------")

launch_sys("R", 3, "futebol", 1090, ids, y, blocs_ids)


print("------------------------------ L 1 ---------------------------------------------------------")

launch_sys("L", 1, "futebol", 1090, ids, y, blocs_ids)

print("------------------------------ L 2 ---------------------------------------------------------")

launch_sys("L", 2, "futebol", 1090, ids, y, blocs_ids)

print("------------------------------ R 3 ---------------------------------------------------------")

launch_sys("L", 3, "futebol", 1090, ids, y, blocs_ids)

print("------------------------------ both 1 ---------------------------------------------------------")

launch_sys("both", 1, "futebol", 1090, ids, y, blocs_ids)

print("------------------------------ both 2 ---------------------------------------------------------")

launch_sys("both", 2, "futebol", 1090, ids, y, blocs_ids)

print("------------------------------ both 3 ---------------------------------------------------------")

launch_sys("both", 3, "futebol", 1090, ids, y, blocs_ids)

