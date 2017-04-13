__author__ = 'alexandre s. cavalcante'

from ner.buildRules import BuildRules
from database.mySQLConnector import MySQLConnector

db = MySQLConnector('memoire', '20060907jl', 'root')
db.rebuild_db()

cat_corpus = 'cinema'
size = 'micro'

with open('../ner/100_stop_words.txt', 'r', encoding='utf-8') as stop_w_file:

    stop_w_list = [stop_w.strip() for stop_w in stop_w_file.readlines()]

br = BuildRules(stop_w_list, '../corpus/_clean_corpus/' + cat_corpus + '_' + size + '.txt', 3, db)

# extract seeds
#br.extract_rules(["Minha Mãe é uma Peça 2", "Assassin's Creed", "xXx: Reativado", "Eu Fico Loko", "Moana - Um Mar de Aventuras", "La La Land - Cantando Estações", "Sing - Quem Canta Seus Males Espanta", "Os Penetras 2 - Quem Dá Mais?", "Sete Minutos Depois da Meia-Noite", "Dominação", "A Criada", "Capitão Fantástico", "Eu, Daniel Blake", "Manchester à Beira-Mar", "Rogue One - Uma História Star Wars", "Elle", "O Apartamento", "Sully - O Herói do Rio Hudson", "Animais Noturnos", "Resident Evil 6: O Capítulo Final", "A Bailarina", "Quatro Vidas de um Cachorro",
 #                 "Beleza Oculta", "Max Steel", "Até o Último Homem", "Os Saltimbancos Trapalhões - Rumo a Hollywood", "Axé: Canto Do Povo De Um Lugar", "Aquarius"], "S")

br.extract_rules(["Star Wars",], "S")

# br.extract_rules(["Capitão América 3: Guerra Civil"], "C")
#
br.extract_potential_nes()
pot_ne_not_treated = br.get_children_items(0)

count = 0
while(len(pot_ne_not_treated) > 0):
    count += 1
    print('loop - ' + str(count) + str(pot_ne_not_treated))
    br.extract_rules(pot_ne_not_treated, "C")

    # analise rules before extract new NE
    br.extract_potential_nes()
    pot_ne_not_treated = br.get_children_items(0)

