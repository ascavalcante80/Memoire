__author__ = 'alexandre s. cavalcante'

from ner_mysql.build_rules_v5 import BuildRules
from database.mySQLConnector import MySQLConnector

db = MySQLConnector()
db.rebuild_db()

cat_corpus = 'cinema'
size = 'full'

with open('100_stop_words.txt', 'r', encoding='utf-8') as stop_w_file:

    stop_w_list = [stop_w.strip() for stop_w in stop_w_file.readlines()]

br = BuildRules(stop_w_list, '../clean_corpus/' + cat_corpus + '_' + size + '.txt', 3)
br.extract_seed_rules(["Esquadrão Suicida", "Procurando Dory", "Café Society", "Aquarius", "Jumanji",
                       "Rogue One - Uma História Star Wars", "Caça-Fantasmas", "Sete Homens e Um Destino",
                       "A Lenda de Tarzan", "Ela Volta na Quinta"])

br.get_potential_NEs()
NEs_not_treated = br.get_potential_NE_not_treated()

count = 0
while(len(NEs_not_treated) > 0):
    count += 1
    print('loop - ' + str(count) + str(NEs_not_treated))
    br.extract_seed_rules(NEs_not_treated, False)
    br.get_potential_NEs()
    NEs_not_treated = br.get_potential_NE_not_treated()

