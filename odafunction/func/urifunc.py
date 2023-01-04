# 
# remote python function, retrievable by file://, http://, with :: function in it
# 

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from nb2workflow.nbadapter import NotebookAdapter
from .. import LocalPythonFunction, Function, LocalValue
from ..utils import iterate_subclasses, repr_trim

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
        logger.info("parsing URI %s", repr_trim(uri))
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
            with open(nba.output_notebook_fn) as f:
                output_nb = json.load(f)

            nba.remove_tmpdir()
            return {
                'output_nb': output_nb,
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



class URIValue(URIFunction, LocalValue):
    """
    nullary function returning value stored as byte content at URI
    """

    # cached = True

    def __init__(self, uri=None, value=None, provenance=None) -> None:
        logger.info("constructing %s from uri=%s value=%s provenance=%s", self.__class__, uri, repr_trim(value), provenance)
        Function.__init__(self, provenance=provenance)

        if uri is None:
            if provenance is None:
                raise NotImplementedError
            else:
                self.construct_uri_from_provenance()
        else:
            self.parse_uri(uri)

        if value is None:
            self.load_func()
        else:
            self.write_to_uri(value)
            self._value = value


    def construct_uri_from_provenance(self):
        # TODO: here, also construct annotations

        codify = lambda p: f"{p.__class__.__name__}_{hashlib.md5(repr(p).encode()).hexdigest()[:8]}"
        
        segments = []
        for p in reversed(self.provenance):
            logger.info(" prov:>> %s", p)

            if p[0] == 'execute':
                segment = p[2].uri + "/" + codify(p[1])
            elif p[1] == 'partial':
                segment = getattr(p[1], 'uri', None)
            else:
                raise NotImplementedError

            
            if segment is None:
                segment = codify(p)
            
            segment = re.sub("::", "/", segment)
            segment = re.sub(r"\.py", "", segment)
            segment = re.sub(r"\.ipynb", "", segment)

            logger.info(" segment:>> %s", segment)

            segments.append(segment)

        if not re.match(r"^((ipynb|py)\+)?file://.*$", segments[0]):
            segments.insert(0, "file://urivalue/")

        self.uri = "/".join(segments)
                
        logger.info("derived uri %s", self.uri)
        self.parse_uri(self.uri)
        

    def write_to_uri(self, value):
        if self.schema != 'file':
            raise NotImplementedError

        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

        with open(self.path, "w") as f:
            json.dump(value, f)


    @property
    def value(self):
        if hasattr(self, '_value'):
            value = self._value
        else:
            value = "not-loaded"
        
        return value


    def load_func_from_local_file(self, path):
        with open(path, "r") as f:
            self._value = json.load(f)


    def __repr__(self) -> str:
        return super().__repr__() + f"[uri: {self.uri} value: {repr_trim(self.value)}]"


    @property
    def constructor_args(self):
        return {'value': self.value, 'uri': self.uri}