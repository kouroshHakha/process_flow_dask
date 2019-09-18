
class Params:

    def __init__(self, params):
        self.params = params

    def __getitem__(self, item):
        return self.params[item]