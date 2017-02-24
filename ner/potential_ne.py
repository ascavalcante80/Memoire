import numpy as np
__author__ = 'alexandre s. cavalcante'

class PotentialNE (object):

    def __init__(self, surface, ne_type, treated=0):
        self.idpotential_ne = -1
        self.surface = surface
        self.ne_type = ne_type
        self.treated = treated
        self.frequency = 0
