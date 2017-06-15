import re

import numpy as np
__author__ = 'alexandre s. cavalcante'

class PotentialNE (object):

    def __init__(self, surface, ne_type, treated=0):
        self.idpotential_ne = -1
        self.surface = surface
        self.ne_type = ne_type
        self.treated = treated
        self.frequency = 0

    def get_escaped(self):

        try:

            surface_escaped = re.sub('(\?|:|;|!|,|\.|-)', '_', self.surface)

        except TypeError:
            print('error linha 379 ')

        surface_escaped = surface_escaped.replace(' ', '_')

        # replacign underscore in the beginning and in the end of string
        if re.match(r'^(_+)(.*?)$', surface_escaped):
            surface_escaped = re.sub(r'^(_+)(.*?)', r' \2', surface_escaped)

        if re.match(r'^(.*?)(_+)$', surface_escaped):
            surface_escaped = re.sub(r'^(.*?)(_+)$', r'\1 ', surface_escaped)

        return surface_escaped