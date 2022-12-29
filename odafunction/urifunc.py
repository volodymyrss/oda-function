# 
# remote python function, retrievable by file://, http://, with :: function in it
# 

import importlib
from . import LocalPythonFunction

import re
import inspect


class URIPythonFunction(LocalPythonFunction):
    def __init__(self, uri) -> None:
        self.uri = uri

        r = re.match("(?P<schema>(http|https|file))://(?P<path>.*)::(?P<funcname>.*)", uri)
        if r is None:
            RuntimeError(f"URI {uri} does not look right")
        else:
            self.schema = r.group('schema')
            self.path = r.group('path')
            self.funcname = r.group('funcname')
            self.load_func()



    def load_func(self):
        if self.schema == "file":
            spec = importlib.util.spec_from_file_location("testmod", self.path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.local_python_function = getattr(module, self.funcname)


    def __repr__(self) -> str:
        return super().__repr__() + f":[{self.uri}]"
