import nltk

__author__ = 'alexandre s. cavalcante'

"""
    This class extends the class nltk.Text and overwrites the method print_concordance() to make it return an array with
     the result.
"""

class NewText(nltk.Text):


    def __init__(self, tokens):
        self.tokens = tokens
        super()

    def print_concordance(self, word, width=75, lines=25):
        """
        Print a concordance for ``word`` with the specified context window.

        :param word: The target word
        :type word: str
        :param width: The width of each line, in characters (default=80)
        :type width: int
        :param lines: The number of lines to display (default=25)
        :type lines: int
        """
        half_width = (width - len(word) - 2) // 2
        context = width // 4   # approx number of words of context
        lines_out = []

        offsets = self.offsets(word)
        if offsets:
            lines = min(lines, len(offsets))
            print("Displaying %s of %s matches:" % (lines, len(offsets)))
            for i in offsets:
                if lines <= 0:
                    break
                left = (' ' * half_width +
                        ' '.join(self._tokens[i-context:i]))
                right = ' '.join(self._tokens[i+1:i+context])
                left = left[-half_width:]
                right = right[:half_width]
                lines_out.append((left, self._tokens[i], right))
                # print(left, self._tokens[i], right)
                lines -= 1
        else:
            print("No matches")
        return lines_out