import os, pickle, nltk, shutil, re

__author__ = 'alexandre s. cavalcante'
__version__= '0.1'

"""
    This script cleans the text files downloaded from globo.com et adorocinema.com. It eliminates the comments pages,
    and write new files with one sentence per line. The first line containing the url is also eliminated.

"""


class TreatCorpus:

    def __init__(self, url_list_path=None):
        pass
        # pass
        # shutil.rmtree('./clean')
        # os.mkdir('./clean')
        # os.mkdir('./clean/futebol')
        # os.mkdir('./clean/cinema')
        # os.mkdir('./clean/tech')
        #
        # if url_list_path is not None:
        #     self.url_list = []
        #
        #     # load pickle url_list
        #     try:
        #         self.url_list = pickle.load(open(url_list_path, 'r'))
        #     except FileNotFoundError:
        #         # create url_list pickle if it doesn't exists
        #         pickle.dump([], open(url_list_path, 'w'))


    def clean_sport_tech_articles(self, path):

        assert path.startswith('../corpus/tech/') or path.startswith('../corpus/futebol/'), path + " is not a sport article"

        # get type futebol or tech
        type = path.split('/')[2]

        article = open(path, 'r', encoding='utf-8').readlines()

        if("populares\n" in article and "recentes\n" in article):

            # don't take the first 10 lines - with menus, etc
            article = article[9:]

            # delete comments from articles - comments a
            for index, line in enumerate(article):
                if(line.startswith('recentes') and index < len(article)-2):
                    if(article[index + 1].startswith('populares')):

                        # delete white lines before 'populares'
                        sent_list = self.format_article(article[:index-6])
                        open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("\n".join(sent_list))
                        break
        else:
            sent_list = self.format_article(article[3:])
            open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("\n".join(sent_list))


    def clean_cinema_articles(self, path):

            assert path.startswith('../corpus/cinema/'), path + " is not a sport article"

            # get type futebol or tech
            type = path.split('/')[2]

            article = open(path, 'r', encoding='utf-8').readlines()

            print(path)
            if(len(article)>4):
                if(article[3] == "Notícias da sua região\n" ):

                    # don't take the first 10 lines - with menus, etc
                    sent_list = self.format_article(article[46:])
                    # open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("\n".join(sent_list))

                else:
                    sent_list = self.format_article(article[6:])
                    # open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("\n".join(sent_list))
            else:
                sent_list = self.format_article(article)

            if(len(sent_list) > 0):
                open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("\n".join(sent_list))


    def format_article(self, article):
        """
        Eliminates double white spaces in the middle of sentences
        :param article: string containing the text of article
        :return: array of string with containing the sentences tokenized.
        """
        article = " ".join(article).replace('\n', ' ')
        article = re.sub(' +', ' ', article)
        sent_list = nltk.sent_tokenize(article, 'portuguese')

        return sent_list


    def get_article_url(self, article_path):

        with (open(article_path, 'r')) as article:
            article = article.readlines()
            if(0 < len(article) ):
                first_line = article[0]

                if first_line.startswith('URL_REF: '):
                    item = first_line.replace('URL_REF: ','').strip()

                    return item


    def concatenate_file(self, path, file_out):

        files_list = os.listdir(path)

        corpus = []
        for file in files_list:
            with (open(path + file, 'r', encoding='utf-8')) as text_file:
                corpus.extend(text_file.readlines())

        with (open(file_out, 'w',encoding='utf-8')) as corpus_out:
            corpus_out.writelines(corpus)


    def get_tokens_NEs(self, raw_text):
        """
        Improves the tokenize function fron NLTK splitting words which have been put together caused by a wrong
        text tokenization.

        :param raw_text: string containing the text to be tokenized
        :return: array of string containing the tokens of the corpus
        """
        all_tokens = []

        temp_tokens = nltk.tokenize.word_tokenize(raw_text.strip(), language='portuguese')

        for token in temp_tokens:

            if re.match('[A-Z]?[a-z]+(\.)[A-Z][a-z]*', token):

                all_tokens.extend(token.split('.'))

            all_tokens.append(token)

        all_tokens[0] = '<begin>' + all_tokens[0]
        all_tokens.insert(len(all_tokens), '<end>')
        return all_tokens


    def get_tokens_rules(self, raw_text):
        """
        Improves the tokenize function fron NLTK splitting words which have been put together caused by a wrong
        text tokenization.

        :param raw_text: string containing the text to be tokenized
        :return: array of string containing the tokens of the corpus
        """
        all_tokens = []

        temp_tokens = nltk.tokenize.word_tokenize(raw_text.strip(), language='portuguese')

        for token in temp_tokens:

            if re.match('[A-Z]?[a-z]+(\.)[A-Z][a-z]*', token):

                all_tokens.extend(token.split('.'))

            all_tokens.append(token)

        return all_tokens
