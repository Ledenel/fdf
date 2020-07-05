import numpy

from fdf.fdf import ArrayInfo


# copied from https://www.python.org/download/releases/2.2/descrintro/#__new__
class Singleton(object):
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass


class TxtBackend(Singleton):
    def load(self, path):
        with open(path, "r") as f:
            return numpy.array([line for line in f])

    def dump(self, path, array):
        with open(path, "w") as f:
            f.writelines(str(item) for item in array)


class ArrayParser(Singleton):
    def init(self):
        self.backend = {
            "txt": TxtBackend,
            "npy": None,
            "empty": None,
        }
        self.row_parser = {
            "__block__": None,
        }
        self.column_transformer = {
            "__categories__": None,
            "__tokenize__": None,
            "__ref__": None,
            "__reindex__": None,
            "__transform__": None,
        }
        self.array_extensions = {
            "__valuemap__": None,
            "__foreignkey__": None,
        }

    def check_syntax(self, array_info: ArrayInfo):
        transformers = []
        for sub_info in array_info.children:
            if sub_info.array_name in self.column_transformer:
                transformers.append(sub_info.array_name)
        # TODO: refactor to raise ArrayFileNameSyntaxError
        assert len(transformers) <= 1, f"Cannot apply multiple column-transformer {transformers} to " \
                                       f"column {array_info} "

    def parse(self, array_info: ArrayInfo):
        pass
