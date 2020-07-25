import ast
import functools
from collections import OrderedDict

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

import gzip
import lzma
import bz2

class NoCompress(Singleton):
    @functools.wraps(open)
    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    def compress(self, data, compresslevel=9):
        return data

    def decompress(self, data):
        return data

class CompressFactory(Singleton):
    def __init__(self):
        self.map = {
            "bz2": bz2,
            "lzma": lzma,
            "gz": gzip,
            None: NoCompress()
        }

import io

class TxtBackend(Singleton):
    def load(self, f):
        f = io.TextIOWrapper(f, encoding="utf-8")
        return numpy.array([ast.literal_eval(line.strip()) for line in f])

    def dump(self, f, array):
        with open(f, "w") as f:
            f.writelines(str(item) for item in array)

def not_implemented(*args, **kwargs):
    raise NotImplementedError()

class ArrayParser(Singleton):
    def init(self):
        self.backend = {
            "txt": TxtBackend(),
            "npy": not_implemented,
            "empty": not_implemented,
            "arrow": not_implemented,
        }
        # self.row_parser = {
        #     "__arrow_chunk__": None,
        # }
        self.array_transform = {
            "__arrow_chunk__": not_implemented,
            "__arrow_list__": not_implemented,#offsets, (values),
            "__arrow_struct__": not_implemented,
            "__arrow_array__": not_implemented, #nulls,(data), metadata.json: type=xxx, buffer_names=[xxx,yyy,zzz]
            "__arrow_dictionary": not_implemented,
        }
        # self.array_extensions = {
        #     "__arrow_dictionary": None,
        #     # "__foreignkey__": None,
        # }

    def check_syntax(self, array_info: ArrayInfo):
        transformers = []
        for sub_info in array_info.children:
            if sub_info.array_name in self.array_transform:
                transformers.append(sub_info.array_name)
        # TODO: refactor to raise ArrayFileNameSyntaxError
        assert len(transformers) <= 1, f"Cannot apply multiple column-transformer {transformers} to " \
                                       f"column {array_info} "

    def parse(self, array_info: ArrayInfo):
        self_array = None
        if array_info.backend is not None:
            with CompressFactory().map[array_info.compression].open(array_info.path, "rb") as f:
                self_array = self.backend[array_info.backend].load(f)
        if array_info.array_name in self.array_transform:
            parsed_dict = {
                v.array_name: self.parse(v) for v in array_info.children
            }
            return self.array_transform[array_info.array_name](self_array, **parsed_dict)
        else:
            for child_info in array_info.children:
                transform = self.parse(child_info)
                self_array = transform(self_array)
            return self_array

class ArrayTransformer:
    def __init__(self, config_array, *kwargs):
        pass

    def __call__(self, parent_array):
        pass
