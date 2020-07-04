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


class ArrayParser(Singleton):
    def init(self):
        self.backend_reader = {
            "txt": None,
            "npy": None,
        }
        self.row_parser = {
            "__block__": None,
        }
        self.column_transformer = {
            "__categories__": None,
            "__tokenize__": None,
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
