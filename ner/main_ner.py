__author__ = 'alexandre s. cavalcante'

from ner.build_rules_v5 import BuildRules
from database.mySQLConnector import MySQLConnector

db = MySQLConnector()
#db.rebuild_db()

cat_corpus = 'cinema'
size = 'full'

with open('100_stop_words.txt', 'r', encoding='utf-8') as stop_w_file:

    stop_w_list = [stop_w.strip() for stop_w in stop_w_file.readlines()]

br = BuildRules(stop_w_list, '../_clean_corpus/' + cat_corpus + '_' + size + '.txt', 5)

# br.extract_ontology_rules(["filme"])

# extract seeds
br.extract_rules(["Minha Mãe é uma Peça 2", "Assassin's Creed", "xXx: Reativado", "Eu Fico Loko", "Moana - Um Mar de Aventuras", "La La Land - Cantando Estações", "Sing - Quem Canta Seus Males Espanta", "Os Penetras 2 - Quem Dá Mais?", "Sete Minutos Depois da Meia-Noite", "Dominação", "A Criada", "Capitão Fantástico", "Eu, Daniel Blake", "Manchester à Beira-Mar", "Rogue One - Uma História Star Wars", "Elle", "O Apartamento", "Sully - O Herói do Rio Hudson", "Animais Noturnos", "Resident Evil 6: O Capítulo Final", "A Bailarina", "Quatro Vidas de um Cachorro",
                  "Beleza Oculta", "Max Steel", "Até o Último Homem", "Os Saltimbancos Trapalhões - Rumo a Hollywood", "Axé: Canto Do Povo De Um Lugar", "Aquarius"])


# extract non seeds
# br.extract_rules(["O bebê de Bridget Jones"], False)
# br.extract_rules(["Neymar", "Cristiano Ronaldo", "Messy", "Gabriel Jesus", "Suárez", "Iniesta", "Piqué", "Ibrahimovic", ])

# br.get_potential_NEs()
# NEs_not_treated = br.get_potential_NE_not_treated()
#
# count = 0
# while(len(NEs_not_treated) > 0):
#     count += 1
#     print('loop - ' + str(count) + str(NEs_not_treated))
#     br.extract_rules(NEs_not_treated, False)
#     br.get_potential_NEs()
#     NEs_not_treated = br.get_potential_NE_not_treated()

