# 
# remote python function, retrievable by file://, http://, with :: function in it
# 

import importlib
import tempfile
from . import LocalPythonFunction

import re
import logging
import requests


logger = logging.getLogger(__name__)

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


    def load_func_from_local_file(self, path):
        logger.info("loading from %s", path)
        spec = importlib.util.spec_from_file_location("testmod", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.local_python_function = getattr(module, self.funcname)

    def load_func(self):
        if self.schema == "file":
            self.load_func_from_local_file(self.path)

        elif self.schema in ["http", "https"]:
            with tempfile.NamedTemporaryFile(suffix=".py") as f:
                f.write(requests.get(f"{self.schema}://{self.path}").content)
                f.flush()
                self.load_func_from_local_file(f.name)

        else:
            raise NotImplementedError


    def __repr__(self) -> str:
        return super().__repr__() + f":[{self.uri}]"
