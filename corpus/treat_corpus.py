import os, pickle, nltk, shutil, re
import regex

__author__ = 'alexandre s. cavalcante'
__version__= '0.1'

"""
    This script cleans the text files downloaded from globo.com et adorocinema.com. It eliminates the users' comments,
    and write new files containing one sentence per line. The first line containing the url is also eliminated.
"""


class TreatCorpus:

    def __init__(self):
        pass

    def build_directories(self, url_list_path=None):
        shutil.rmtree('./clean')
        os.mkdir('./clean')
        os.mkdir('./clean/futebol')
        os.mkdir('./clean/cinema')
        os.mkdir('./clean/tech')

        if url_list_path is not None:
            self.url_list = []

            # load pickle url_list
            try:
                self.url_list = pickle.load(open(url_list_path, 'r'))
            except FileNotFoundError:
                # create url_list pickle if it doesn't exists
                pickle.dump([], open(url_list_path, 'w'))


    def clean_sport_tech_articles(self, path):

        assert path.startswith('../corpus/tech/') or path.startswith('../corpus/futebol/'), path + \
                                                                                            " is not a sport article"

        # get type futebol or tech
        type = path.split('/')[2]

        article = open(path, 'r', encoding='utf-8').readlines()

        sent_list = self.format_article(article[3:])

        clean_lines =[]
        for line in sent_list:

            line = self.clean_text(line)

            count = 0
            while re.match('(.*?\s.{3,})(\.|!|;|:|\?)+(([A-Z]|É|Ó|Ú|À|Á|Í).+)', line, flags=re.UNICODE) and "vol." not in line.lower():
                line = re.sub('(.*?\s.{3,})(\.|!|;|:|\?)+(([A-Z]|É|Ó|Ú|À|Á|Í).+)', r'\1' +r'\2' + "\n" + r'\3', line)
                count +=1
                if count > 2000:
                    count = 0
                    break

            while re.match('(.*?[a-z]{3,})(([A-Z]|É|Ó|Ú|À|Á|Í).{3,})', line, flags=re.UNICODE) and "AdoroCinema" not in line:
                line = re.sub('(.*?[a-z]{3,})(([A-Z]|É|Ó|Ú|À|Á|Í).*)', r'\1' + " " +r'\2', line) # split captila joint to lower ex blablaToto
                count +=1
                if count > 2000:
                    break

            if "\n" in line:
                clean_lines.extend(line.split("\n"))
            else:
                clean_lines.append(line)

        open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("\n".join(clean_lines))


    def clean_cinema_articles(self, path, rep_quotes=None):

            #assert path.startswith('../corpus/cinema/'), path + " is not a sport article"

            # get type futebol or tech
            type = path.split('/')[2]

            article = open(path, 'r', encoding='utf-8').readlines()

            print(path)
            if(len(article) > 4 and re.match(".*?\d\d/\d\d/\d\d\d\d \d\dh\d\d",  article[4])):

                # delete lines from Globo.com articles ex. Por G1, 16/01
                sent_list = article[5:]
            else:

                sent_list = self.format_article(article)

            clean_lines = []
            # iterate over lines to split joint lines like ex. do cinema blabla.Hoje foi blabla
            for line in sent_list:

                line = self.clean_text(line)

                # replacing all the quotes for
                if rep_quotes is not None:
                    #quotes may be used has features for the machine learning treatment.

                    # line = line.replace("'",'"') # simples quotes
                    line = line.replace("\"", rep_quotes)  # double quotes
                    line = line.replace("“", rep_quotes)
                    line = line.replace("”", rep_quotes)
                    line = line.replace("″", rep_quotes)
                    line = line.replace("‘", rep_quotes)
                    line = line.replace("’", rep_quotes)
                    line = line.replace("′", rep_quotes)


                count = 0
                while re.match('(.*?\s.{3,})(\.|!|;|:|\?)+(([A-Z]|É|Ó|Ú|À|Á|Í).+)', line, flags=re.UNICODE) and "vol." not in line.lower():
                    line = re.sub('(.*?\s.{3,})(\.|!|;|:|\?)+(([A-Z]|É|Ó|Ú|À|Á|Í).+)', r'\1' +r'\2' + "\n" + r'\3', line)
                    count +=1
                    if count > 50:
                        count = 0
                        break

                while re.match('(.*?[a-z]{3,})(([A-Z]|É|Ó|Ú|À|Á|Í).{3,})', line, flags=re.UNICODE) and "AdoroCinema" not in line:
                    line = re.sub('(.*?[a-z]{3,})(([A-Z]|É|Ó|Ú|À|Á|Í).*)', r'\1' + " " +r'\2', line) # split capital joint to lower ex blablaToto
                    count +=1
                    if count > 50:
                        break

                if "\n" in line:
                    clean_lines.extend(line.split("\n"))
                else:
                    clean_lines.append(line)

                clean_lines = [line.strip() + "\n" for line in clean_lines if line.strip() != ""]

            if(len(clean_lines) > 0):
                open("./clean/" + type + "/" + path.split("../corpus/" + type + "/")[1], 'w', encoding='utf-8').write("".join(clean_lines[1:]))


    def format_article(self, article):
        """
        Eliminates double white spaces in the middle of sentences and normalized abbreviations.
        :param article: string containing the text of article
        :return: array of string with containing the sentences tokenized.

        """
        article = " ".join(article).replace('\n', ' ')

        # replacing dot in abbreviations - its avoit problems in the tokenizations
        article = re.sub('([A-Z])\.([A-Z])\.([A-Z])?\.?([A-Z])?\.?([A-Z])?\.?([A-Z])?\.?', r'\1\2\3\4\5\6', article)

        article = re.sub(' +', ' ', article)
        sent_list = nltk.sent_tokenize(article, 'portuguese')

        return sent_list


    def clean_text(self, line):

        # removing inseparable space
        line = line.replace(' ', ' ')
        return line

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

                # normalize quotes symbols
                for line in text_file.readlines():

                    corpus.append(line)

        with (open(file_out, 'w',encoding='utf-8')) as corpus_out:
            corpus_out.writelines(corpus)


    def get_tokens(self, raw_text):
        """
        Improves the tokenize function fron NLTK splitting words which have been put together due a wrong
        separation

        :param raw_text: string containing the text to be tokenized
        :return: array of string containing the tokens of the corpus
        """
        all_tokens = []
        temp_tokens = nltk.word_tokenize(raw_text)

        for token in temp_tokens:

            must_split = re.split('.+?[A-Z]?[a-z]+(\.)[A-Z][a-z]+.+?', token)
            if len(must_split) > 1:
                all_tokens.append(must_split[0] + '.', token)
                all_tokens.append(must_split[1])
            all_tokens.append(token)


    def fix_sent_tokenization(self, path_corpus_in):

        with(open("./" + path_corpus_in, 'r', encoding='utf-8')) as corpus_file:

            corpus_str = corpus_file.readlines()

            corpus_clean = regex.sub(r'(.*?\s\p{Lu}.)\n', r'\1', "".join(corpus_str))

        with(open('CLEAN_' + path_corpus_in, 'w', encoding='utf-8')) as corpus_out:

            corpus_out.write(corpus_clean)

