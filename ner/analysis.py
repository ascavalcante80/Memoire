import operator

from database.mySQLConnector import MySQLConnector
import pickle

class Analyze_NE(object):

    def __init__(self, connector):
        assert isinstance(connector, MySQLConnector)
        self.conn = connector

    def get_rules_seed(self):
        set_seeds = self.conn.get_potential_ne_where('type', 'S')

        seeds_id = []
        for pot_ne in set_seeds:
            seeds_id.append(pot_ne.idpotential_ne)

        set_rules_id = []

        for id in seeds_id:
            set_rules_id.extend(self.conn.get_item_pot_ne_rule(id, "potential_ne_idpotential_ne"))
        return set_rules_id

connector = MySQLConnector('memoire', '20060907jl', 'root', host='localhost')
# ===============================================================================================================
# building set of rules for all potential_ne
dic_id_rules={}
for pot_ne in connector.get_all_elements('potential_ne'):
    ids_rules = []
    for child_rules in connector.get_potential_ne_rules(['potential_ne_idpotential_ne'], [pot_ne.idpotential_ne]):

        ids_rules.append(child_rules[1])
    dic_id_rules[pot_ne.idpotential_ne] = ids_rules

# ===============================================================================================================


# ana = Analyze_NE(connector)

# seed_rules_ids = ana.get_rules_seed()
# child_rules = connector.get_potential_ne_rules(['potential_ne_idpotential_ne'], [30])

threshold = 20

groups_ids = {}

for ids in dic_id_rules.keys():
    size = len(dic_id_rules[ids])

    if size < 2:
        continue
    set_similar_ids = []

    for ids2 in dic_id_rules.keys():

        if ids2 == ids:
            continue

        commons = set_dics = set(dic_id_rules[ids]) & set(dic_id_rules[ids2])

        size_commons = len(commons)

        if size_commons == 0:
            continue

        pourcent = (size_commons / size) * 100

        if pourcent > threshold:
            set_similar_ids.append(ids2)

    groups_ids[ids] = set_similar_ids

    # print(str(pourcent) + "%")

for key in groups_ids:

    # print(str(key) + "---> " + str(groups_ids[key]))

    pot_ne = connector.get_potential_ne_where("idpotential_ne",key)[0]

    nes_simi = groups_ids[key]

    if len(nes_simi) > 0:

        print(pot_ne.surface)
        for id in nes_simi:
            pot_ne = connector.get_potential_ne_where("idpotential_ne",id)[0]
            print("--"+ pot_ne.surface)

        print('\n')
