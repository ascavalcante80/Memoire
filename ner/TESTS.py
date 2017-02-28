# from nltk.tokenize.moses import MosesDetokenizer
# from nltk import word_tokenize
#
# from tagger import Tagger
#
# sentence = 'Começa hoje a estreia de Star Wars no cinema.'
# tagger = Tagger('portuguese', '/home/alexandre/treetagger/cmd/')
# POS, lemmas, tokens_treetagger = tagger.tag_sentence(sentence.strip())
# tokens_nlkt = word_tokenize(sentence, 'portuguese')
#
# lemmas_rule = ['o', 'estreia', 'de', '"']
#
# joint_rule = "<sep>".join(lemmas_rule)
# joint_sent = "<sep>".join(lemmas)
# if joint_rule in joint_sent:
#
#     left_context = joint_sent.split(joint_rule)[1]
#     pot_ne_index = len(lemmas) - len(left_context.split("<sep>")) + 1


nltk = ['Começa', 'hoje', 'a', 'estreia', 'de', 'Star', 'Wars', 'no', 'cinema', '.']
treetagger = ['Começa', 'hoje', 'no', 'a', 'estreia', 'de', 'Star', 'Wars', 'em', 'o', 'cinema', '.']

diff = True
changes_limit = len(treetagger) - len(nltk)
diff_index = 0
while changes_limit > 0:
    for index, token in enumerate(nltk):
        index -= diff_index

        if nltk[index] != treetagger[index] and changes_limit > 0:
            treetagger.pop(index)
            changes_limit -= 1
            diff_index += 1
        elif nltk[index] != treetagger[index]:
            treetagger[index] = nltk[index]



# count_quote = 1
# # treat quotes
# escaped_tokens = []
# for word in tokens:
#
#     if word == '"' and count_quote % 2 != 0:
#         escaped_tokens.append('<open_quote>')
#         count_quote +=1
#     elif word == '"' and count_quote % 2 == 0:
#         escaped_tokens.append('<close_quote>')
#         count_quote += 1
#     else:
#         escaped_tokens.append(word)
#
# pot_ne = tokens[pot_ne_index:]
#
# detokenizer = MosesDetokenizer()
# s = detokenizer.detokenize(lemmas, return_str=True)
print('o')

