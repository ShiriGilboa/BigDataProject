from abc import ABC, abstractmethod


# Each parser is a set of values from the same type, implemented as a list type with additional "parse method"
class Parser:
    def __init__(self, data):
        self.data = []
        self.parse(data)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, index):
        return self.data[index]

    @abstractmethod
    def parse(self, data):
        pass
