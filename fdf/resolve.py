import ast
import functools
import json
import re
from abc import ABCMeta, abstractmethod

import numpy
import pyarrow

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


class NoCompress:
    @classmethod
    @functools.wraps(open)
    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    @classmethod
    def compress(self, data, compresslevel=9):
        return data

    @classmethod
    def decompress(self, data):
        return data


class MemoryMapCompress:
    @functools.wraps(pyarrow.memory_map)
    def open(self, *args, **kwargs):
        return pyarrow.memory_map(*args, **kwargs)

    @classmethod
    def compress(self, data, compresslevel=9):
        return data

    @classmethod
    def decompress(self, data):
        return data


class CompressFactory(Singleton):
    def init(self):
        self.map = {
            "bz2": bz2,
            "lzma": lzma,
            "gz": gzip,
            None: NoCompress,
            "mmap": MemoryMapCompress,
        }


import io


class Backend(Singleton, metaclass=ABCMeta):
    @abstractmethod
    def load(self, f):
        pass

    @abstractmethod
    def dump(self, f, array):
        pass


class TxtBackend(Backend):
    def load(self, f, encoding="utf-8"):
        f = io.TextIOWrapper(f, encoding=encoding)
        return numpy.array([ast.literal_eval(line.strip()) for line in f])

    def dump(self, f, array):
        with pyarrow.output_stream(f) as f:
            f.writelines(repr(item) for item in array)


class JsonBackend(Backend):
    def load(self, f, encoding="utf-8"):
        f = io.TextIOWrapper(f, encoding=encoding)
        return json.load(f)

    def dump(self, f, array):
        json.dump(array, f)


class ArrowBufferBackend(Backend):
    def load(self, f):
        if hasattr(f, "read_buffer") and callable(f.read_buffer):
            return f.read_buffer()
        else:
            return pyarrow.input_stream(f).read_buffer()

    def dump(self, f, buffer_like):
        with pyarrow.output_stream(f) as out_f:
            out_f.write(buffer_like)


def not_implemented(*args, **kwargs):
    raise NotImplementedError()


class ArrayParser(Singleton):
    def init(self):
        self.backend = {
            "txt": TxtBackend(),
            "npy": not_implemented,
            "empty": not_implemented,
            "json": JsonBackend(),
            "jsonl": not_implemented,
            "arrow": not_implemented,
            "buffer": ArrowBufferBackend(),
        }
        # self.row_parser = {
        #     "__arrow_chunk__": None,
        # }
        self.array_transform = {
            "__arrow_chunk__": not_implemented,
            "__arrow_list__": not_implemented,  # offsets, (values),
            "__arrow_struct__": not_implemented,
            "__arrow_array__": not_implemented,  # nulls,(data), metadata.json: type=xxx, buffer_names=[xxx,yyy,zzz]
            "__arrow_dictionary__": not_implemented,
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


_MAP_TO_FACTORY = {
    "fixed_size_binary": "binary"
}

import pyparsing as pyp


def arrow_type_to_dict(t: pyarrow.DataType):
    ctor, _, args = re.match(r"^([a-zA-Z0-9]+)(\[(.*?)\])?$", str(t)).groups()
    if ctor in {'timestamp'}:
        t: pyarrow.lib.TimestampType
        args = [t.unit, t.tz]
    elif args.isdigit():
        args = int(args)
    elif args is None:
        args = []
    if ctor in _MAP_TO_FACTORY:
        ctor = _MAP_TO_FACTORY[ctor]
    return {"name": ctor, "args": args}


class ArrowExtractor(Singleton):
    def __init__(self, directory):
        self.directory = directory
        self.field_order = {
            2: ["null", "data"],
            3: ["null", "offset", "data"],
        }

    def schedule(self, array: pyarrow.Array, info: ArrayInfo):
        if array.type.num_children == 0 and str(array.type) != "directory":  # primitive, contains null_bitmap, data
            array_meta = info.new_child("__arrow_array__")
            field_orders = self.field_order[array.type.num_buffers]
            for name, buffer in zip(field_orders, array.buffers()):
                array_meta.new_child("values",
                                     backend="buffer",
                                     compression="mmap",
                                     data=buffer).move_to(self.directory)
            array_meta.new_child("metadata", backend="json", data={
                **arrow_type_to_dict(array.type),  # type, args,
                "field_order": field_orders  # field_order
            }).move_to(self.directory)
            return info
        raise NotImplementedError()
