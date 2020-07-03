"""Main module."""
import collections
import os
from glob import glob
from typing import List, Optional, Iterator, Iterable

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

    def __init__(self, backend: Optional[str], prefixes: Optional[Iterable[str]] = None, path: str = None):
        self.path = path
        self.prefixes = tuple(prefixes) or (ArrayInfo.new_name(),)
        self.backend = backend
        self.children = []

    @property
    def array_name(self):
        return self.prefixes[-1]

    @property
    def column_name(self):
        return self.prefixes[0]

    def __str__(self):
        return f"{'.'.join(self.prefixes)}.{self.backend}"

    def __hash__(self):
        return hash(self._object_view())

    def _object_view(self):
        unique_view = tuple(self.prefixes), self.backend, self.path
        return unique_view

    def __eq__(self, other):
        return self._object_view() == other._object_view()

    def parent(self):
        is_child = len(self.prefixes) > 1
        return ArrayInfo(backend=None, prefixes=self.prefixes[:-1]) if is_child else None

    @classmethod
    def from_path(cls, path: str):
        resolved_path = path.translate(cls.CHARS_FILTER)
        # FIXME: warn when resolved_path is modified.
        try:
            results = cls.FILE_NAME.parseString(resolved_path, parseAll=True)
        except pyp.ParseException as e:
            raise FileNameInvalidError(e)
        backend = results["backend"]
        prefixes = list(results["ext"])
        # FIXME: panic when str(arrayInfo) is not resolved_path.
        return cls(backend, prefixes, path)


class FDataFrame:
    pass


def parse_array_info_tree(array_infos: Iterable[ArrayInfo]):
    #FIXME: panic while multiple prefixes appears (means that multiple backends of array are provided.)
    cached_array_infos = {array_info.prefixes: array_info for array_info in array_infos}
    roots = set()
    cache_is_dirty = True
    while cache_is_dirty:
        cache_is_dirty = False
        for item in list(cached_array_infos.values()):
            item: ArrayInfo
            gen_parent = item.parent()
            if gen_parent is not None:
                cached_parent = cached_array_infos.get(gen_parent.prefixes, None)
                if cached_parent is not None:
                    cached_parent.children.append(item)
                else:
                    cached_array_infos[gen_parent.prefixes] = gen_parent
                    cache_is_dirty = True
            else:
                roots.add(item)
    root_list = list(roots)
    root_list.sort(key=lambda array_info: array_info.column_name)
    return root_list


def parse(fdf_dir):
    dirs = glob(fdf_dir, recursive=True)
