
class Sentence(object):

    def __init__(self, surface, line_nb):

        self.surface = surface
        self.line_nb = line_nb

        # use the same value until a new value is assigned
        self.line_escaped = surface
