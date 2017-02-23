import pymysql
from ner.rule import Rule
from ner.potential_ne import PotentialNE

__author__ = 'alexandre s. cavalcante'


class MySQLConnector:

    def __init__(self, database, password, user, host='localhost', port=3306, charset="utf8"):

        self.__database = database
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__charset = charset

        self.__conn = pymysql.connect(self.__host, self.__port, self.__user, self.__password, self.__database,
                                      self.__charset, use_unicode=True, autocommit=True)

    def rebuild_db(self):
        try:
            try:
                query = """
                -- MySQL Workbench Forward Engineering

                SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
                SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
                SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

                -- -----------------------------------------------------
                -- Schema memoire
                -- -----------------------------------------------------
                DROP SCHEMA IF EXISTS `memoire` ;

                -- -----------------------------------------------------
                -- Schema memoire
                -- -----------------------------------------------------
                CREATE SCHEMA IF NOT EXISTS `memoire` DEFAULT CHARACTER SET utf8 ;
                USE `memoire` ;

                -- -----------------------------------------------------
                -- Table `memoire`.`rules`
                -- -----------------------------------------------------
                DROP TABLE IF EXISTS `memoire`.`rules` ;

                CREATE TABLE IF NOT EXISTS `memoire`.`rules` (
                  `idrules` INT NOT NULL,
                  `surface` VARCHAR(1000) NOT NULL,
                  `orientation` VARCHAR(1) NOT NULL,
                  `lemmas` VARCHAR(1000) NULL,
                  `POS` VARCHAR(45) NULL,
                  `frequency` INT NULL DEFAULT 0,
                  `treated` TINYINT(1) NULL DEFAULT 0,
                  PRIMARY KEY (`idrules`))
                ENGINE = InnoDB
                DEFAULT CHARACTER SET = utf8;


                -- -----------------------------------------------------
                -- Table `memoire`.`potential_ne`
                -- -----------------------------------------------------
                DROP TABLE IF EXISTS `memoire`.`potential_ne` ;

                CREATE TABLE IF NOT EXISTS `memoire`.`potential_ne` (
                  `idpotential_ne` INT NOT NULL,
                  `surface` VARCHAR(500) NOT NULL,
                  `frequency` INT NULL DEFAULT 0,
                  `treated` TINYINT(1) NULL DEFAULT 0,
                  `type` VARCHAR(1) NOT NULL,
                  PRIMARY KEY (`idpotential_ne`))
                ENGINE = InnoDB
                DEFAULT CHARACTER SET = utf8;


                -- -----------------------------------------------------
                -- Table `memoire`.`potential_ne_has_rules`
                -- -----------------------------------------------------
                DROP TABLE IF EXISTS `memoire`.`potential_ne_has_rules` ;

                CREATE TABLE IF NOT EXISTS `memoire`.`potential_ne_has_rules` (
                  `potential_ne_idpotential_ne` INT NOT NULL,
                  `rules_idrules` INT NOT NULL,
                  PRIMARY KEY (`potential_ne_idpotential_ne`, `rules_idrules`),
                  INDEX `fk_potential_ne_has_rules_rules1_idx` (`rules_idrules` ASC),
                  INDEX `fk_potential_ne_has_rules_potential_ne_idx` (`potential_ne_idpotential_ne` ASC),
                  CONSTRAINT `fk_potential_ne_has_rules_potential_ne`
                    FOREIGN KEY (`potential_ne_idpotential_ne`)
                    REFERENCES `memoire`.`potential_ne` (`idpotential_ne`)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION,
                  CONSTRAINT `fk_potential_ne_has_rules_rules1`
                    FOREIGN KEY (`rules_idrules`)
                    REFERENCES `memoire`.`rules` (`idrules`)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION)
                ENGINE = InnoDB
                DEFAULT CHARACTER SET = utf8;


                SET SQL_MODE=@OLD_SQL_MODE;
                SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
                SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

                """

                cur = self.__getConnection()
                cur.execute(query)
                cur.close()
                return True

            except pymysql.err.MySQLError:
                cur.close()
                # todo insert logger
                return False
        except Exception:
            # todo insert logger
            return False

    def insert_rule(self, rule):
        """
        insert the data from the object rule in the database. It return the idrules, if the rules was correctly
        inserted. Otherwise, it returns -1.
        :param rule: instance of Rule
        :return: int idrules
        """
        try:
            # variable to hold the object connection
            cur = None

            if not isinstance(rule, Rule) or rule is None:
                return -1

            # check if the rule has already been inserted in the database
            rule_result = self.get_rule(rule)
            if rule_result is not None:
                # rule already in the DB, return its id
                return rule_result.rule_id

            if len(rule.POS) == 0 or len(rule.lemmas) == 0:
                return -1
                # todo insert logger

            POS = "<sep>".join(rule.POS)
            lemmas = "<sep>".join(rule.lemmas)

            try:
                cur = self.__getConnection()

                query = "INSERT INTO `" + self.__database + "`.`rules` (`surface`, `orientation`," \
                                                            " `lemmas`, `POS`, `treated`) VALUES ('"\
                        + pymysql.escape_string(rule.surface) + "', '" + pymysql.escape_string(rule.orientation)\
                        + "', '" + pymysql.escape_string(lemmas) + "', '" + pymysql.escape_string(POS) +\
                        "', '" + str(rule.treated) + "');"

                cur.execute(query)

            except pymysql.err.IntegrityError:
                # todo insert logger
                cur.close()
                return -1

            # inserted worked, get id and return it
            rule_id = cur.lastrowid
            cur.close()
            return rule_id

        except Exception:
            return -1
            # todo insert logger

    def insert_potential_ne(self, potential_ne):
        """
        inserts an object PotentialNE in the database and return its id, if the object was correctly inserted. Otherwise
        it returns -1.
        :param potential_ne: object PotentialNE
        :return: int idpotential_ne
        """
        try:
            # variable to hold the object connection
            cur = None

            if not isinstance(potential_ne, PotentialNE) or potential_ne.surface is None:
                return None

            pot_ne_result = self.get_potential_NE(potential_ne.surface)

            if pot_ne_result is not None:
                return pot_ne_result.id

            try:
                cur = self.__getConnection()
                query = "INSERT INTO `memoire`.`potential_ne` (`surface`, `frequency`, `treated`, `type`) VALUES ('" \
                        + pymysql.escape_string(potential_ne.surface) + "', '" + str(potential_ne.frequency) + "', '" \
                        + str(potential_ne.treated) + "', '" + potential_ne + "');"
                cur.execute(query)

            except pymysql.err.IntegrityError:
                # todo insert logger
                cur.close()
                return -1

            # get id for the item just inserted
            potential_NE_id = cur.lastrowid
            cur.close()
            return potential_NE_id
        except Exception:
            # todo insert logger
            return -1







    def insert_relation_ne_rule(self, idrules, idpotential_ne):
        """
        insert the relation rule has potential_ne in the database. It return True if the relation was correctly
        inserted, and False otherwise.
        :param idrules: int
        :param idpotential_ne: int
        :return: boolean
        """

        try:
            cur = None

            if idrules is None or idpotential_ne is None or not isinstance(idrules, int) \
                    or not isinstance(idpotential_ne, int):
                return -1
            try:
                cur = self.__getConnection()

                query = 'INSERT INTO `' + self.__database + '`.`potential_ne_has_rules` ' \
                                                            '(`potential_ne_idpotential_ne`, `rules_idrules`) VALUES ' \
                                                            '(' + str(idpotential_ne) + ', ' + str(idrules) + ');'
                cur.execute(query.replace("'", "''"))
                cur.close()
                return True

            except pymysql.err.IntegrityError:
                # todo insert logger
                cur.close()
                return False

        except Exception:
            # todo insert logger
            return False

    def get_not_treated_NE(self, treated=0):

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.PotentialNE WHERE PotentialNE.treated = " + str(treated) + ";")

        except pymysql.err.IntegrityError:
            pass
            return False

        potential_NEs = []
        for lines in cur._rows:

            # todo converter o methodo no main extract_rules() para utilizar un array como o array abaixo, melhor POO
            # convert is_seed to bool
            # pot_NE = PotentialNE(lines[1], bool(lines[3]))
            # pot_NE.id = lines[0]
            # pot_NE.treated = lines [4]
            #
            # potential_NEs.append(pot_NE)
            #
            potential_NEs.append(lines[1])

        return potential_NEs


    def get_potential_NE(self, potential_NE_surface):

        if potential_NE_surface is None or potential_NE_surface == '':
            return None

        try:
            cur = self.__getConnection()
            cur.execute('SELECT * FROM ner_cult.PotentialNE WHERE PotentialNE.potential_NE_surface ="' + pymysql.escape_string(potential_NE_surface) + '" ;')

        except pymysql.err.IntegrityError:
            pass
            return None

        if len(cur._rows) > 0:

            pot_NE = PotentialNE(cur._rows[1])
            pot_NE.id = cur._rows[0]

            return pot_NE
        else:
            return None


    def get_all_NE(self):

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * from ner_cult.PotentialNE;")

        except pymysql.err.IntegrityError:
            pass
            return False

        potential_NEs = []
        for line_db in cur._rows:
            pot_NE = PotentialNE(line_db[1])
            pot_NE.id = line_db[0]
            pot_NE.frequency = line_db[2]

            if line_db[3] == 1:
                is_seed = True
            else:
                is_seed = False

            pot_NE.is_seed = is_seed
            pot_NE.treated = True

            potential_NEs.append(pot_NE)

        return potential_NEs


    def get_rules_by_type(self, seed=True):

        if seed:
            type_rule = 1
        else:
            type_rule = 0

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.Rule inner join ner_cult.Rule_has_PotentialNE on ner_cult.Rule_has_PotentialNE.Rule_idRule=ner_cult.Rule.idRule inner join ner_cult.PotentialNE on ner_cult.PotentialNE.idPotentialNE=ner_cult.Rule_has_PotentialNE.PotentialNE_idPotentialNE where ner_cult.PotentialNE.is_seed="+ str(type_rule)+";")

        except pymysql.err.IntegrityError:
            pass
            return False

        return cur._rows


    def get_not_treated_rules(self):

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.Rule WHERE Rule.treated = 0;")

        except pymysql.err.IntegrityError:
            pass
            return False

        rules = []
        for db_line in cur._rows:
            rule = Rule(db_line[1], db_line[2], '')

            rule.rule_id = db_line[0]
            rule.freq = db_line[3]
            rule.production = db_line[4]
            rule.variety = db_line[5]
            rule.seed_production = db_line[6]
            rule.treated = db_line[7]

            rules.append(rule)

        return rules


    def get_all_rules_ontology(self):

        list_rules = []
        try:
            cur = self.__getConnection()
            cur.execute(
                "SELECT * FROM ner_cult.Rule_Ontology;")

        except pymysql.err.IntegrityError:
            pass
            return None

            # verify if result is not empty
        if len(cur._rows):

            for row in cur._rows:

                rule = Rule(row[1], row[2], '')

                rule.freq = row[3]
                rule.lemmas = row[4]
                rule.POS = row[7]
                rule.rule_id = row[0]

                list_rules.append(rule)

        else:
            return None  # empty result the rules is not in the database
        return list_rules


    def get_rule_ontonlogy(self, rule):

        if not isinstance(rule, Rule) or rule is None:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.Rule_Ontology WHERE Rule_Ontology.rule_surface = '" + pymysql.escape_string(rule.surface) + "' and Rule_Ontology.orientation ='" + rule.orientation + "';")

        except pymysql.err.IntegrityError:
            pass
            return None

        # verify if result is not empty
        if len(cur._rows):

            rule = Rule(cur._rows[0][1], cur._rows[0][2], '')

            rule.freq = cur._rows[0][3]
            rule.production = cur._rows[0][4]
            rule.variety = cur._rows[0][5]
            rule.seed_production = cur._rows[0][6]
            rule.treated = cur._rows[0][7]
            rule.rule_id = cur._rows[0][0]

            return rule
        else:
            return None # empty result the rules is not in the database


    def get_rule(self, rule):

        if not isinstance(rule, Rule) or rule is None:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.Rule WHERE Rule.rule_surface = '" + pymysql.escape_string(rule.surface) + "' and Rule.orientation ='" + rule.orientation + "';")

        except pymysql.err.IntegrityError:
            pass
            return None

        # verify if result is not empty
        if len(cur._rows):

            rule = Rule(cur._rows[0][1], cur._rows[0][2], '')

            rule.freq = cur._rows[0][3]
            rule.production = cur._rows[0][4]
            rule.variety = cur._rows[0][5]
            rule.seed_production = cur._rows[0][6]
            rule.treated = cur._rows[0][7]
            rule.rule_id = cur._rows[0][0]

            return rule
        else:
            return None # empty result the rules is not in the database


    def get_item_rules(self, potential_NE):

        if not isinstance(potential_NE, PotentialNE):
            return None

        if potential_NE.id == -1:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("SELECT Rule_idRule FROM ner_cult.Rule_has_PotentialNE WHERE PotentialNE_idPotentialNE=" + str(potential_NE.id) + ";")

        except pymysql.err.IntegrityError:
            cur.close()
            return None


        rules_ids = []
        # verify if result is not empty
        if len(cur._rows) > 0:

            for line_db in cur._rows:

                rules_ids.append(line_db[0])
            return rules_ids
        else:
            return None


    def get_rules_items(self, rule):

        if not isinstance(rule, Rule):
            return None

        if rule.rule_id == -1:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("SELECT PotentialNE_idPotentialNE FROM ner_cult.Rule_has_PotentialNE WHERE Rule_idRule=" + str(rule.rule_id) + ";")

        except pymysql.err.IntegrityError:
            cur.close()
            return None


        pot_ne_ids = []
        # verify if result is not empty
        if len(cur._rows) > 0:

            for line_db in cur._rows:

                pot_ne_ids.append(line_db[0])
            return pot_ne_ids
        else:
            return None


    def get_potential_NE(self, potential_NE):


        if potential_NE == '' or potential_NE is None:
            return None

        potential_NE = potential_NE.strip()

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.PotentialNE WHERE PotentialNE.potential_NE_surface = '" + pymysql.escape_string(potential_NE) + "';")

        except pymysql.err.IntegrityError:
            cur.close()
            return None

        # verify if result is not empty
        if len(cur._rows):
            pot_ne = PotentialNE(cur._rows[0][1])
            pot_ne.id = cur._rows[0][0]

            return pot_ne
        else:
            return None


    def get_rule_production(self, idRule):

        if idRule is None:
            return None

        if idRule == -1:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("SELECT PotentialNE.potential_NE_surface FROM ner_cult.PotentialNE inner join "
                        "ner_cult.Rule_has_PotentialNE "
                        "on ner_cult.Rule_has_PotentialNE.PotentialNE_idPotentialNE = ner_cult.PotentialNE.idPotentialNE "
                        "where ner_cult.Rule_has_PotentialNE.Rule_idRule=" + str(idRule) + ";")

        except pymysql.err.IntegrityError:
            cur.close()
            return 0

        if len(cur._rows) > 0:

            pot_NEs = []
            for line_db in cur._rows:
                pot_NEs.append(line_db[0])
            cur.close()
            return pot_NEs
        else:
            cur.close()
            return None


    def get_seed_production(self, idRule, seeds_ids):

        if not len(seeds_ids) > 0:
            return 0

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.Rule_has_PotentialNE WHERE Rule_has_PotentialNE.Rule_idRule = " + str(idRule) + ";")

        except pymysql.err.IntegrityError:
            cur.close()
            return 0

        # verify if result is not empty
        total = len(cur._rows)
        cur.close()

        if total > 0:
            count = 0
            for db_line in cur._rows:

                if db_line[1] in seeds_ids:
                    count += 1

            seed_production = count / total
            return seed_production
        else:
            return 0


    def updated_rule_seed_production(self, idRule, seed_production):

        if idRule is None or seed_production is None:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("UPDATE `ner_cult`.`Rule` SET `seed_production`=" + str(seed_production) + " WHERE `idRule`='" + str(idRule) + "';")

        except pymysql.err.IntegrityError:
            cur.close()
            return None

        return True


    def updated_rule_production(self, idRule, production):

        if idRule is None or production is None:
            return None

        try:
            cur = self.__getConnection()
            cur.execute("UPDATE `ner_cult`.`Rule` SET `production`=" + str(production) + " WHERE `idRule`='" + str(idRule) + "';")

        except pymysql.err.IntegrityError:
            cur.close()
            return None

        return True



    def updated_ontology_freq(self, idrule,freq):

        if idrule is None or freq is None:
            return False

        try:
            cur = self.__getConnection()
            cur.execute("UPDATE `ner_cult`.`Rule_Ontology` SET `frequency`='" + str(freq) + "' WHERE `idRule`='" + str(idrule) + "';")

        except pymysql.err.IntegrityError:
            pass
            return False

        return True


    def updated_rule(self, rule):

        if rule is None or not isinstance(rule, Rule):
            return False

        if rule.treated:
            treated = '1'
        else:
            treated = '0'

        try:
            cur = self.__getConnection()
            cur.execute("UPDATE `ner_cult`.`Rule` SET `rule_surface`='" + pymysql.escape_string(rule.surface) + "', `orientation`='" + rule.orientation + "', `frequency`='" + str(rule.freq) + "', `production`='" + str(rule.production) + "', `variety`='" + str(rule.variety) + "', `seed_production`='" + str(rule.seed_production) + "', `treated`='" + treated + "' WHERE `idRule`='" + str(rule.rule_id) + "';")


        except pymysql.err.IntegrityError:
            pass
            return False

        return True


    def updated_potential_NE(self, potential_NE):

        if potential_NE is None and not isinstance(potential_NE, PotentialNE):
            return False

        if potential_NE.treated:
            treated = '1'
        else:
            treated = '0'

        if potential_NE.is_seed:
            is_seed = '1'
        else:
            is_seed = '0'


        try:
            cur = self.__getConnection()
            cur.execute("UPDATE `ner_cult`.`PotentialNE` SET `potential_NE_surface`='" +
                        pymysql.escape_string(potential_NE.surface)
                        + "', `frequency`='" + str(potential_NE.frequency) + "', `is_seed`='"
                        + is_seed + "', `treated`='" + treated +
                        "' WHERE `idPotentialNE`='" + str(potential_NE.id) + "';")

        except pymysql.err.IntegrityError:

            cur.close()
            return False

        cur.close()
        return True


    def __getConnection(self):

        cur = self.__conn.cursor()
        cur.execute("use ner_cult")

        return cur


    def analyse_preposition(self):

        query = 'SELECT orientation,rule_lemmas, POS FROM ner_cult.Rule;'
        conn = self.__getConnection()
        conn.execute(query)

        if len(conn._rows) > 0:

            verbes= {}
            prep={}
            noums={}
            punct = {}
            for line_db in conn._rows:


                if line_db[0] == 'R':
                    lemmas = line_db[1].split('<sep>')[0]
                    pos = line_db[2].split('<sep>')[0]
                else:
                    lemmas = line_db[1].split('<sep>')[-1]
                    pos = line_db[2].split('<sep>')[-1]

                if pos.startswith('V') and lemmas in verbes.keys():
                    verbes[lemmas]+= 1
                elif pos.startswith('V'):
                    verbes[lemmas] = 1
                elif pos.startswith('N') and lemmas in noums.keys():
                    noums[lemmas] += 1
                elif pos.startswith('N'):
                    noums[lemmas] = 1
                elif pos.startswith('SP') and lemmas in prep.keys():
                    prep[lemmas] += 1
                elif pos.startswith('SP'):
                    prep[lemmas] =1
                elif pos.startswith('F') and lemmas in punct.keys():
                    punct[lemmas] += 1
                elif pos.startswith('F'):
                    punct[lemmas] =1

            conn.close()
            return verbes,prep, noums

        conn.close()

