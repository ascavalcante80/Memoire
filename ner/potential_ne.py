import numpy as np
__author__ = 'alexandre s. cavalcante'

class PotentialNE (object):

    def __init__(self, surface, ne_type):
        self.id = -1
        self.surface = surface
        self.frequency = 0
        self.ne_type = ne_type
        self.treated = False


