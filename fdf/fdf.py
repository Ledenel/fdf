"""Main module."""
import collections
import os
from glob import glob
from typing import List, Optional, Iterator

import pyparsing as pyp


class ExtArray:
    def __init__(self, array, extensions=None):
        self._array = array
        self.extensions = extensions or []
        self.parent = None
        for ext in extensions:
            ext.parent = self

    def __len__(self):
        return len(self._array)

    def __getitem__(self, item):
        return self._array[item]


class MaskTrans(collections.abc.Mapping):
    MAX_LEN = 65535

    def __len__(self) -> int:
        return MaskTrans.MAX_LEN - len(self.valid_chars)

    def __iter__(self) -> Iterator[int]:
        return iter(set(range(MaskTrans.MAX_LEN)) - self.valid_chars)

    def __init__(self, valid_chars: str, replaced_by: str):
        self.replaced_by = replaced_by
        self.valid_chars = set(ord(x) for x in valid_chars)

    def __getitem__(self, item: int):
        if item not in self.valid_chars:
            return self.replaced_by
        raise LookupError


class FileNameInvalidError(Exception):
    def __init__(self, e: pyp.ParseException):
        super().__init__("\n" + pyp.ParseException.explain(e))


class ArrayInfo:
    next_id = 1
    BACKEND = pyp.Word(pyp.alphanums).setResultsName("backend")
    ext_chars = pyp.alphanums + "-_"
    EXTNAME = pyp.Word(ext_chars).setResultsName("ext", listAllMatches=True)
    FILE_NAME = pyp.OneOrMore(EXTNAME + ".") + BACKEND
    CHARS_FILTER = MaskTrans(valid_chars=ext_chars + ".", replaced_by="-")

    @classmethod
    def new_name(cls):
        name = "unnamed-{}".format(cls.next_id)
        cls.next_id += 1
        return name

    def __init__(self, backend: str, ext_chain: Optional[List[str]] = None, path=None):
        self.path = path
        self.ext_chain = ext_chain or [ArrayInfo.new_name()]
        self.backend = backend

    @property
    def array_name(self):
        return self.ext_chain[-1]

    @property
    def column_name(self):
        return self.ext_chain[0]

    @classmethod
    def from_path(cls, path: str):
        resolved_path = path.translate(cls.CHARS_FILTER)
        try:
            results = cls.FILE_NAME.parseString(resolved_path, parseAll=True)
        except pyp.ParseException as e:
            raise FileNameInvalidError(e)
        backend = results["backend"]
        ext_chain = list(results["ext"])
        return cls(backend, ext_chain, path)


class FDataFrame:
    pass


def parse(fdf_dir):
    dirs = glob(fdf_dir, recursive=True)
