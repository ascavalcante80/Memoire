__author__ = 'alexandre s. cavalcante'

import pymysql
from ner.rule import Rule
from ner.potential_ne import PotentialNE

class MySQLConnector:

    def __init__(self):

        # informacao importante o autocommit tem que estar setado com True, pois caso contrario o dados nao sao salvos no db!!!
        self.__conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='20060907jl', db='ner_cult',  charset="utf8", use_unicode=True, autocommit=True)


    def insert_rule_ontology(self, rule):

        rule_result = self.get_rule_ontonlogy(rule)

        if rule_result is not None:

            self.updated_ontology_freq(rule_result.rule_id, rule_result.freq + 1)

            return rule_result.rule_id

        if rule.has_number():
            has_number = '1'
        else:
            has_number = '0'


        if rule.has_punctuation():
            punct = '1'
        else:
            punct = '0'

        # delete article in final and initial position according to orientation
        POS, lemmas = rule.get_tags()

        if len(POS) > 0 and len(lemmas) > 0 and rule.orientation == 'L':

            if POS[-1].startswith('D'):

                POS = POS[:-1]
                lemmas = lemmas[:-1]

            elif '+D' in POS[-1]:
                POS[-1] = POS[-1].split('+')[0]
                lemmas[-1] = lemmas[-1].split('+')[0].strip()

            # case where only the lemmas have contraction
            elif '+o' in lemmas[-1] or '+a' in lemmas[-1]:
                lemmas[-1] = lemmas[-1].split('+')[0].strip()



        lemmas = "<sep>".join(lemmas)
        POS = "<sep>".join(POS)

        try:
            cur = self.__getConnection()
            query = 'INSERT INTO `ner_cult`.`Rule_Ontology` (`rule_surface`, `orientation`, `rule_lemmas`, POS) VALUES ("' + pymysql.escape_string(rule.surface) + '", "' + rule.orientation + '", "' + pymysql.escape_string(lemmas)  + '", "' + pymysql.escape_string(POS)  + '");'

            print(query)
            cur.execute(query)

        except pymysql.err.IntegrityError:
            pass
            cur.close()
            return -1
        rule_id = cur.lastrowid
        cur.close()
        return rule_id


    def insert_rule(self, rule, is_ontology_rule=False):

        rule_result = self.get_rule(rule)
        if rule_result is not None:
            return rule_result.rule_id

        if rule.treated:
            treated = '1'
        else:
            treated = '0'

        if rule.has_number():
            has_number = '1'
        else:
            has_number = '0'


        if rule.has_punctuation():
            punct = '1'
        else:
            punct = '0'

        if is_ontology_rule:
            # delete article in final and initial position according to orientation
            POS, lemmas = rule.get_tags()

            if rule.orientation == 'L':

                # todo verificar tamanho das listas antes de começar a divisão, verificar os caso de 'deste'
                if POS[-1].startswith('D'):
                    POS = POS[:-1]
                    lemmas = lemmas[:-1]

                elif '+D' in POS[:-1]:
                    POS[:-1] = POS[-1].split('+')[0]
                    lemmas[:-1] = lemmas[-1].split('+')[0]


        else:
            POS, lemmas = rule.get_tags()

        POS = "<sep>".join(POS)
        lemmas = "<sep>".join(lemmas)

        try:
            cur = self.__getConnection()

            query = 'INSERT INTO `ner_cult`.`Rule` (`rule_surface`, `orientation`, `frequency`, `production`, `variety`, `seed_production`, `treated`, `rule_lemmas`, `punct`, `has_number`, `POS`) VALUES ("' + pymysql.escape_string(rule.surface) + '", "' + rule.orientation + '", ' + str(rule.freq) + ', ' + str(rule.production) + ',' + str(rule.variety) + ',' + str(rule.seed_production) + ',' + treated + ',"' + pymysql.escape_string(lemmas) + '",' + punct + ',' + has_number + ',"' + pymysql.escape_string(POS) + '");'

            print(query)
            cur.execute(query)
            # cur.execute(query.replace("'", "''"))

        except pymysql.err.IntegrityError:
            pass
            cur.close()
            return -1
        rule_id = cur.lastrowid
        cur.close()
        return rule_id


    def insert_potential_NE(self, pot_NE):

        if pot_NE.surface is None or len(pot_NE.surface) == 1 or pot_NE.surface == '':
            return None

        pot_NE_result = self.get_potential_NE(pot_NE.surface)

        if pot_NE_result is not None:
            return pot_NE_result.id

        if pot_NE.treated:
            treated = '1'
        else:
            treated = '0'

        if pot_NE.is_seed:
            is_seed = '1'
        else:
            is_seed = '0'

        try:
            cur = self.__getConnection()

            query = 'INSERT INTO `ner_cult`.`PotentialNE` (`potential_NE_surface`, `frequency`, `is_seed`, `treated`) VALUES ("' + pymysql.escape_string(pot_NE.surface.strip()) + '", ' + str(pot_NE.frequency) + ',' + is_seed + ',' + treated + ');'
            cur.execute(query.replace("'", "''"))

        except pymysql.err.IntegrityError:

            cur.close()
            return -1

        # get id for the item just inserted
        potential_NE_id = cur.lastrowid
        cur.close()
        return potential_NE_id


    def insert_relation_NE_rule(self, rule_id, potential_ne_id):

        if rule_id is None or potential_ne_id is None:
            return -1
        try:
            cur = self.__getConnection()

            query = 'INSERT INTO `ner_cult`.`Rule_has_PotentialNE` (`Rule_idRule`, `PotentialNE_idPotentialNE`) VALUES (' + str(rule_id) + ', ' + str(potential_ne_id) + ');'
            cur.execute(query.replace("'", "''"))
            cur.close()
            return True

        except pymysql.err.IntegrityError:

            cur.close()
            return -1


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


    def rebuild_db(self):

        query = """
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema ner_cult
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `ner_cult` ;

-- -----------------------------------------------------
-- Schema ner_cult
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `ner_cult` DEFAULT CHARACTER SET utf8 ;
USE `ner_cult` ;

-- -----------------------------------------------------
-- Table `ner_cult`.`Rule`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`Rule` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`Rule` (
  `idRule` INT NOT NULL AUTO_INCREMENT,
  `rule_surface` VARCHAR(1000) NOT NULL,
  `orientation` VARCHAR(1) NOT NULL,
  `frequency` FLOAT NULL,
  `production` FLOAT NULL,
  `variety` FLOAT NULL,
  `seed_production` FLOAT NULL,
  `treated` TINYINT(1) NOT NULL,
  `rule_lemmas` VARCHAR(1000) NULL,
  `punct` VARCHAR(2) NULL,
  `has_number` TINYINT(1) NULL,
  `POS` VARCHAR(1000) NULL,
  PRIMARY KEY (`idRule`, `rule_surface`, `orientation`, `treated`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `ner_cult`.`PotentialNE`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`PotentialNE` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`PotentialNE` (
  `idPotentialNE` INT NOT NULL AUTO_INCREMENT,
  `potential_NE_surface` VARCHAR(400) NOT NULL,
  `frequency` FLOAT NOT NULL,
  `is_seed` TINYINT(1) NOT NULL,
  `PotentialNEcol` VARCHAR(45) NULL,
  `treated` TINYINT(1) NOT NULL,
  PRIMARY KEY (`idPotentialNE`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `ner_cult`.`Rule_has_PotentialNE`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`Rule_has_PotentialNE` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`Rule_has_PotentialNE` (
  `Rule_idRule` INT NOT NULL,
  `PotentialNE_idPotentialNE` INT NOT NULL,
  PRIMARY KEY (`Rule_idRule`, `PotentialNE_idPotentialNE`),
  INDEX `fk_Rule_has_PotentialNE_PotentialNE1_idx` (`PotentialNE_idPotentialNE` ASC),
  INDEX `fk_Rule_has_PotentialNE_Rule_idx` (`Rule_idRule` ASC),
  CONSTRAINT `fk_Rule_has_PotentialNE_Rule`
    FOREIGN KEY (`Rule_idRule`)
    REFERENCES `ner_cult`.`Rule` (`idRule`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `fk_Rule_has_PotentialNE_PotentialNE1`
    FOREIGN KEY (`PotentialNE_idPotentialNE`)
    REFERENCES `ner_cult`.`PotentialNE` (`idPotentialNE`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `ner_cult`.`Rules_Gold`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`Rules_Gold` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`Rules_Gold` (
  `idRules_Gold` INT NOT NULL AUTO_INCREMENT,
  `rule` VARCHAR(200) CHARACTER SET 'utf8' NOT NULL,
  `score` INT NULL DEFAULT 0,
  `production_freq` INT NULL DEFAULT 0,
  `orientation` VARCHAR(1) NULL,
  PRIMARY KEY (`idRules_Gold`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `ner_cult`.`Rule_Ontology`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`Rule_Ontology` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`Rule_Ontology` (
  `idRule` INT NOT NULL AUTO_INCREMENT,
  `rule_surface` VARCHAR(1000) NOT NULL,
  `orientation` VARCHAR(1) NOT NULL,
  `frequency` INT NULL DEFAULT 0,
  `rule_lemmas` VARCHAR(1000) NOT NULL,
  `punct` VARCHAR(2) NULL,
  `has_number` TINYINT(1) NULL,
  `POS` VARCHAR(1000) NULL,
  PRIMARY KEY (`idRule`, `rule_lemmas`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

        """

        conn = self.__getConnection()
        conn.execute(query)
        conn.close()


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

