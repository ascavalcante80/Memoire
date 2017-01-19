__author__ = 'alexandre s. cavalcante'

from database.mySQLConnector import MySQLConnector
# analyse high seed production


db = MySQLConnector()
dic_prod= {}
#
# for idRule in range(1,65937):
#     prod = db.get_seed_production(idRule, [1,2,3,4,5,6,7,8,9,10])
#
#     if prod > 0:
#         dic_prod[idRule] = prod
#
# # write seed_production in the db
# print('write seed_production in the db')
#
# for idRule in dic_prod.keys():
#
#     if not db.updated_rule_seed_production(idRule, dic_prod[idRule]):
#         print('Erro com ' + str(idRule))


for idRule in range(1,65937):
    prod = db.get_rule_production(idRule)

    if prod > 0:
        dic_prod[idRule] = prod

# write seed_production in the db
print('write seed_production in the db')

for idRule in dic_prod.keys():

    if not db.updated_rule_production(idRule, dic_prod[idRule]):
        print('Erro com ' + str(idRule))

