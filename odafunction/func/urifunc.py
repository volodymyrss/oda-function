# 
# remote python function, retrievable by file://, http://, with :: function in it
# 

import hashlib
import importlib.util
import tempfile
from nb2workflow.nbadapter import NotebookAdapter
from .. import LocalPythonFunction, Function, LocalValue
from ..utils import iterate_subclasses

import re
import logging
import requests


logger = logging.getLogger(__name__)


class URIFunction(Function):
    suffix="py"

    def __init__(self, uri) -> None:
        self.parse_uri(uri)
        self.load_func()
    

    def parse_uri(self, uri):
        r = re.match(r"^((?P<modifier>(ipynb|py))\+)?(?P<schema>(http|https|file))://(?P<path>.*?)(::(?P<funcname>.*))?$", uri)
        if r is None:            
            raise RuntimeError(f"URI {uri} does not look right")
        else:
            logger.info("parsed uri %s as %s", uri, r.groupdict())
            self.uri = uri
            self.modifier = r.group('modifier')
            self.schema = r.group('schema')
            self.path = r.group('path')
            self.funcname = r.group('funcname')



    @staticmethod
    def from_uri(uri):
        for cls in iterate_subclasses(URIFunction):
            try:
                # TODO: not only first!
                return cls(uri)
            except RuntimeError as e:
                logger.info("can not parse %s as %s", uri, cls)
        
        raise RuntimeError(f"unable to parse URI {uri}")


    def load_func(self):        
        if self.schema == "file":
            self.load_func_from_local_file(self.path)

        elif self.schema in ["http", "https"]:
            with tempfile.NamedTemporaryFile(suffix="." + self.suffix) as f:
                self.content = requests.get(f"{self.schema}://{self.path}").content
                f.write(self.content)
                f.flush()
                self.load_func_from_local_file(f.name)

        else:
            raise NotImplementedError    


    def load_func_from_local_file(self, path):
        raise NotImplementedError


class URIPythonFunction(URIFunction, LocalPythonFunction):
    suffix="py"
    

    def __init__(self, uri) -> None:
        URIFunction.__init__(self, uri)

    def load_func_from_local_file(self, path):
        logger.info("loading from %s", path)

        # TODO: does module name cause collisions?
        spec = importlib.util.spec_from_file_location("mod" + hashlib.md5(path.encode()).hexdigest()[:8], path)

        if spec is None:
            raise RuntimeError(f"unable to load module from {path}")
        else:
            module = importlib.util.module_from_spec(spec)

            if spec.loader is None:
                raise RuntimeError(f"spec.loader is None for {path}")
            else:
                spec.loader.exec_module(module)
                self.local_python_function = getattr(module, self.funcname)



    def __repr__(self) -> str:
        return super().__repr__() + f":[{self.uri}]"



class URIipynbFunction(URIPythonFunction):
    suffix = "ipynb"

    def load_func_from_local_file(self, path):
        nba = NotebookAdapter(path)
        
        logger.info("parameter definitions: %s", nba.extract_parameters())
        logger.info("output definitions: %s", nba.extract_output_declarations())        

        def local_python_function():
            nba = NotebookAdapter(path)
            nba.execute({}, inplace=getattr(self, 'inplace', False))
            output = nba.extract_output()
            nba.remove_tmpdir()
            return {
                'output_nb': None,
                'output_values': output
            }

        self.local_python_function = local_python_function


class TransformURIFunction(LocalPythonFunction):
    def __init__(self, provenance=None) -> None:
        super().__init__(self.local_python_function, provenance)

    @staticmethod
    def local_python_function(from_func: URIPythonFunction, to_type: type, new_loc: str):
        r = re.match("(?P<schema>(http|https|file))://(?P<path>.*)", new_loc)

        if r is None:
            raise RuntimeError(f"can not load this, unknown pattern: {new_loc}")
        else:
            assert r.group('schema') == 'file'

            with open(r.group('path'), "wb") as f:        
                f.write(from_func.content)

            logger.info("storing function to %s", r.group('path'))
            
            return to_type(new_loc + "::" + from_func.funcname)


def repr_lim(o, lim=30):
    s = str(o)    
    if len(s) > lim:
        s = f"{s[:lim]}...({len(s)})"
    
    return s

class URIValue(URIFunction, LocalValue):
    """
    nullary function returning value stored as byte content at URI
    """

    # cached = True

    def __init__(self, uri, value=None) -> None:        
        self.parse_uri(uri)

        if value is None:
            self.load_func()
        else:
            self.write_to_uri(value)
            self._value = value


    def write_to_uri(self, value):
        if self.schema != 'file':
            raise NotImplementedError

        if isinstance(value, str):
            value = value.encode()

        with open(self.path, "wb") as f:
            f.write(value)


    @property
    def value(self):
        if hasattr(self, '_value'):
            value = self._value
        else:
            value = "not-loaded"

        if isinstance(value, str):
            value = value.encode()

        return value



    def load_func_from_local_file(self, path):
        with open(path, "rb") as f:
            self._value = f.read()


    def __repr__(self) -> str:
        return super().__repr__() + f"[uri: {self.uri} value: {repr_lim(self.value)}]"

    # def dumps(self):
    #     return json.dumps({'value': self.value, 'class': self.__class__.__name__})

    # @classmethod    
    # def from_s(cls, s):
    #     return cls(json.loads(s)['value'])

    # @classmethod    
    # def from_f(cls, f):
    #     return cls(json.load(f)['value'])
