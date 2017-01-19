import numpy as np
__author__ = 'alexandre s. cavalcante'

class PotentialNE (object):

    def __init__(self, surface, is_seed=False):
        self.id = -1
        self.surface = surface
        self.frequency = 0
        self.is_seed = is_seed
        self.treated = False

        self._reliability_mean = 0
        self._set_rules_confidence = [] # stores the confidence of each rules the NE occurs

    def add_reliability(self, rule_confidence):

        # append new score of confidence
        self._set_rules_confidence.append(float(rule_confidence))

        # update the mean
        self._reliability_mean = np.mean(self._set_rules_confidence)




