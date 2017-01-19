__author__ = 'alexandre s. cavalcante'

import pymysql
from ner_mysql.rule import Rule
from ner_mysql.potential_ne import PotentialNE

class MySQLConnector:

    def __init__(self):

        # informacao importante o autocommit tem que estar setado com True, pois caso contrario o dados nao sao salvos no db!!!
        self.__conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='20060907jl', db='ner_cult',  charset="utf8", use_unicode=True, autocommit=True)


    def insert_rule(self, rule):

        rule_result = self.get_rule(rule)
        if rule_result is not None:
            return rule_result.rule_id

        if rule.treated:
            treated = '1'
        else:
            treated = '0'

        try:
            cur = self.__getConnection()

            query = 'INSERT INTO `ner_cult`.`Rule` (`rule_surface`, `orientation`, `frequency`, `production`, `variety`, `seed_production`, `treated`) VALUES ("' + pymysql.escape_string(rule.surface) + '", "' + rule.orientation + '", ' + str(rule.freq) + ', ' + str(rule.production) + ',' + str(rule.variety) + ',' + str(rule.seed_production) + ',' + treated + ');'

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

            # todo converter o methodo no main extract_seed_rules() para utilizar un array como o array abaixo, melhor POO
            # convert is_seed to bool
            # pot_NE = PotentialNE(lines[1], bool(lines[3]))
            # pot_NE.id = lines[0]
            # pot_NE.treated = lines [4]
            #
            # potential_NEs.append(pot_NE)
            #
            potential_NEs.append(lines[1])

        return potential_NEs


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


    def get_not_treated_rules(self):

        try:
            cur = self.__getConnection()
            cur.execute("SELECT * FROM ner_cult.Rule WHERE Rule.treated = 0;")

        except pymysql.err.IntegrityError:
            pass
            return False

        rules = []
        for db_line in cur._rows:
            rule = Rule(db_line[1], db_line[2])

            rule.rule_id = db_line[0]
            rule.freq = db_line[3]
            rule.production = db_line[4]
            rule.variety = db_line[5]
            rule.seed_production = db_line[6]
            rule.treated = db_line[7]

            rules.append(rule)

        return rules


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

            rule = Rule(cur._rows[0][1], cur._rows[0][2])

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
-- MySQL Script generated by MySQL Workbench
-- dim. 18 sept. 2016 13:15:05 CEST
-- Model: New Model    Version: 1.0
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
CREATE SCHEMA IF NOT EXISTS `ner_cult` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE `ner_cult` ;

-- -----------------------------------------------------
-- Table `ner_cult`.`Rule`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`Rule` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`Rule` (
  `idRule` INT NOT NULL AUTO_INCREMENT,
  `rule_surface` VARCHAR(200) NOT NULL,
  `orientation` VARCHAR(1) NOT NULL,
  `frequency` FLOAT NULL,
  `production` FLOAT NULL,
  `variety` FLOAT NULL,
  `seed_production` FLOAT NULL,
  `treated` TINYINT(1) NOT NULL,
  PRIMARY KEY (`idRule`, `rule_surface`, `orientation`, `treated`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


-- -----------------------------------------------------
-- Table `ner_cult`.`PotentialNE`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ner_cult`.`PotentialNE` ;

CREATE TABLE IF NOT EXISTS `ner_cult`.`PotentialNE` (
  `idPotentialNE` INT NOT NULL AUTO_INCREMENT,
  `potential_NE_surface` VARCHAR(400) NOT NULL,
  `frequency` FLOAT NOT NULL,
  `is_seed` TINYINT(1) NOT NULL,
  `treated` TINYINT(1) NOT NULL,
  PRIMARY KEY (`idPotentialNE`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


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
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Rule_has_PotentialNE_PotentialNE1`
    FOREIGN KEY (`PotentialNE_idPotentialNE`)
    REFERENCES `ner_cult`.`PotentialNE` (`idPotentialNE`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_general_ci;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

        """

        conn = self.__getConnection()
        conn.execute(query)
        conn.close()