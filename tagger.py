import os
import re
import pickle
import hashlib

import gc


class Tagger(object):

    def __init__(self, language, dic_path, path=None):

        self.path = path
        self.language = language
        self.dic_path = dic_path
        self.hash_gen = hashlib.md5()
        if not os.path.isdir(dic_path):
            os.makedirs(dic_path)

    def tag_sentence(self, sentence, verbose=False):

        # create key for dict of tags
        key_temp = str(sentence.line_nb) + sentence.line_escaped.strip()
        self.hash_gen.update(key_temp.encode())
        key_dic = self.hash_gen.hexdigest()

        try:
            self.corpus_tags = pickle.load(open(self.dic_path + key_dic[0] + key_dic[1] + key_dic[2] + key_dic[3] + key_dic[4] + '_corpus_tagged.pk', 'rb'))
        except FileNotFoundError:
            self.corpus_tags = {}
        except EOFError:
            print("Interrup execution - pickle seems to be corrupted")
            exit(1)


        if key_dic not in self.corpus_tags.keys():

            sentence.line_escaped = re.sub("\".(?<![a-z])", " QuotesR ", sentence.line_escaped, flags=re.IGNORECASE)
            sentence.line_escaped = re.sub("(?![a-z]).\"", " QuotesL ", sentence.line_escaped, flags=re.IGNORECASE)

            if '"' in sentence.line_escaped:
                sentence.line_escaped = sentence.line_escaped.replace('"', '\\"')

            if self.path is not None:
                tree_tagger_cmd = 'echo "' + sentence.line_escaped + '" | ' + self.path + 'tree-tagger-' + self.language
            else:
                tree_tagger_cmd = 'echo "' + sentence.line_escaped + '" | tree-tagger-' + self.language
            out_put = [line.split("\t")for line in os.popen(tree_tagger_cmd).read().split("\n")][:-1]

            if verbose:
                print(tree_tagger_cmd)
                print(out_put)

            tokens = [item[0] for item in out_put]
            pos = [item[1] for item in out_put]
            lemmas = [item[2] for item in out_put]
            self.corpus_tags[key_dic] = [tokens, pos, lemmas]

            pickle.dump(self.corpus_tags, open(self.dic_path + key_dic[0] + key_dic[1] + key_dic[2] + key_dic[3] + key_dic[4] + '_corpus_tagged.pk', 'wb'))

        else:
            # print ("using tag already saved for line " + sentence.surface )
            tree_tagger_result = self.corpus_tags[key_dic]
            tokens = tree_tagger_result[0]
            pos = tree_tagger_result[1]
            lemmas = tree_tagger_result[2]

        gc.collect()

        return pos, lemmas, tokens

    def get_lemmas(self, sentence, verbose=False):

        tree_tagger_cmd = 'echo \"' + sentence.surface + '\" | ' + self.path + 'tree-tagger-' + self.language
        out_put = [line.split("\t")for line in os.popen(tree_tagger_cmd).read().split("\n")][:-1]

        if verbose:
            print(tree_tagger_cmd)
            print(out_put)

        lemmas = [item[0] for item in out_put]

        return lemmas

    def convert_dic_key(self, sentence):
        hash_gen = hashlib.md5()
        # create key for dict of tags
        key_temp = str(sentence.line_nb) + sentence.line_escaped.strip()
        hash_gen.update(key_temp.encode())
        return self.hash_gen.hexdigest()
