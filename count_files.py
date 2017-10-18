import sys
import os.path
import regex

files_read=[]
qtd_articles = {}

full_corpus = ""

def read_file(path_in):

        file_in = open(path_in, "r" ).readlines(0)
        try:

            if not file_in[0].startswith("URL_REF:"):
                return
                print(file)

            # domain = file_in[0].replace("URL_REF: http://", "").split("/")[0]
            # domain = file_in[0].replace("URL_REF: https://", "").split("/")[0]



            domain = regex.sub("URL_REF: https?://", "", file_in[0]).split("/")[0]

            if domain in qtd_articles.keys():
                qtd_articles[domain] += 1
            else:
                qtd_articles[domain] = 1

        except IndexError:
            pass

path = "/home/alexandre/Documents/memoire/CORPUS_FINAL/ALL_CINEMA/"
files = os.listdir(path)


for index, file in enumerate(files):
    # read_file(path + file)
    print(file)

    full_corpus = open(path+ file, "r").readlines()
    full_corpus.append("\n\n------------------------------------------------------------------------------------------------------\n\n")

    open("full_corpus.txt", "a").writelines(full_corpus)


# for key in qtd_articles:
#     print(key + " : " + str(qtd_articles[key]))