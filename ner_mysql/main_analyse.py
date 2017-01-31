__author__ = 'alexandre s. cavalcante'

from ner_mysql.build_rules_v5 import BuildRules

cat_corpus = 'cinema'
size = 'full'

with open('100_stop_words.txt', 'r', encoding='utf-8') as stop_w_file:

    stop_w_list = [stop_w.strip() for stop_w in stop_w_file.readlines()]

br = BuildRules(stop_w_list, '../clean_corpus/' + cat_corpus + '_' + size + '.txt', 2)

# br.calculate_frequencies()
#
items_group = br.get_items_production()
br.build_bin_groups(items_group)
br.analyse_bin_groups()