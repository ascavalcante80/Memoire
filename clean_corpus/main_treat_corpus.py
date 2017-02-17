from clean_corpus.treat_corpus import TreatCorpus
import os

c = TreatCorpus()

path = '../corpus/'
# reading folders in corpus
folders = [folder for folder in os.listdir(path)]


files = []
for folder in folders:

    print(folder)
    if(folder != 'cinema'):

        for article in os.listdir(path + folder):
            c.clean_sport_tech_articles(path + folder + "/" + article)

    if(folder == 'cinema'):
        for article in os.listdir(path + folder):
            if article.endswith("~") or article.startswith("."):
                continue

            c.clean_cinema_articles(path + folder + "/" + article, '"')


path_conca = './clean/'
# c.concatenate_file(path_conca + 'futebol/', 'futebol_full.txt')

c.concatenate_file(path_conca + 'cinema/', 'cinema_full.txt')
c.fix_sent_tokenization('cinema_full.txt')