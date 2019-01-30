import csv
from itertools import iteritems


def memodict(f):
    """ Memoization decorator for a function taking a single argument """
    class memodict(dict):
        def __missing__(self, key):
            ret = self[key] = f(key)
            return ret
    return memodict().__getitem__


class Mapper:
    dictionary = {}
    filename = 'mnemonics.csv'

    def __init__(self, filename=None):
        if filename:
            with open(filename) as file:
                reader = csv.reader(file)
                self.dictionary = dict(reader)
            self.filename = filename

        self.prefixes = list('$*?!<>_-')
        self.suffixes = list('qwertyuiopasdfghjklzxcvbnm123456789')
        self.mnemonic = self.mnemonics_generator()

    def filter(self, symbol):
        if len(symbol) > 2:
            return self.get_mnemonic(symbol)
        else:
            return symbol

    def get_mnemonic(self, symbol):
        if symbol in self.dictionary:
            return self.dictionary[symbol]
        else:
            mnemonic = next(self.mnemonic)
            self.dictionary[symbol] = mnemonic
            return mnemonic

    def mnemonics_generator(self):
        for prefix in self.prefixes:
            for suffix in self.suffixes:
                mnemonic = prefix + suffix
                yield mnemonic

    @memodict
    def get_symbol(self, mnemonic):
        for s, m in self.dictionary.iteritems():
            if m == mnemonic:
                return s

    def save(self):
        try:
            with open(self.filename, 'w', newline='') as file:
                w = csv.writer(file)
                w.writerows(self.dictionary.items())
        except PermissionError:
            print('file {} is being used by another programme'.format('mnemonics.csv'))

    @classmethod
    def load_file(cls, filename=None):
        if not filename:
            filename = cls.filename
        return cls(filename=filename)
