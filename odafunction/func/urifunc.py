# 
# remote python function, retrievable by file://, http://, with :: function in it
# 

import hashlib
import inspect
import os
import rdflib
import importlib.util
import json
from pathlib import Path
import tempfile
from typing import Any
from nb2workflow.nbadapter import NotebookAdapter
from .. import LocalPythonFunction, Function, LocalValue, Executor
from ..utils import iterate_subclasses, repr_trim

import re
import logging
import requests


logger = logging.getLogger(__name__)


class FuncJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Function) or isinstance(obj, Executor):
            return f"[{obj.__class__.__name__}:{getattr(obj, 'uri', '')}]"
        else:
            return json.JSONEncoder.default(self, obj)


class URIFunction(Function):
    """
    function which can be identified by URI
    partial call produces new URI from provenance
    """

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


    @property
    def uri(self):
        return self._uri


    @uri.setter
    def uri(self, uri):
        self._uri = rdflib.URIRef(uri)


    def parse_uri(self, uri):
        logger.info("parsing URI %s", repr_trim(uri))
        r = re.match(r"^((?P<modifier>(ipynb|py))\+)?(?P<schema>(http|https|file))://(?P<path>.*?)(::(?P<funcname>.*?))?(@(?P<revision>.*))?$", uri)
        if r is None:            
            raise RuntimeError(f"URI {uri} does not look right")
        else:
            logger.info("parsed uri %s as %s", uri, r.groupdict())
            self.uri = uri
            self.modifier = r.group('modifier')
            self.schema = r.group('schema')
            self.path = r.group('path')
            self.funcname = r.group('funcname') 
            self.revision = r.group('revision')



    @staticmethod
    def from_uri(uri):
        for cls in iterate_subclasses(URIFunction):
            if cls in [Function, URIFunction]:
                continue

            try:
                # TODO: not only first!
                return cls(uri=uri)
            except RuntimeError as e:
                logger.info("can not parse %s as %s due to %s", uri, cls, repr(e))
        
        raise RuntimeError(f"unable to parse URI {uri}")


    # TODO: this might rather belong to an partial executor
    def construct_uri_from_provenance(self):
        # TODO: here, also construct annotations
        logger.info("prov:\n %s", json.dumps(self.provenance, indent=4, sort_keys=True, cls=FuncJSONEncoder))
                
        self.uri = uri_from_provenance(self.provenance)
                
        logger.info("derived uri %s", self.uri)
        self.parse_uri(self.uri)



def codify(p): 
    if hasattr(p, 'uri'):        
        c = re.sub("::", "/", p.uri)
        c = re.sub(r"\.py", "", c)
        c = re.sub(r"\.ipynb", "", c)
        return c

    else:
        return f"{p.__class__.__name__}_{hashlib.md5(repr(p).encode()).hexdigest()[:8]}"


def uri_segments_from_provenance(p):
    segments = []

    logger.info(" [red]prov:>>[/] %s", p)
        
    
    if p is None:
        return []
    elif p[0] == 'execute':
        segments.append(codify(p[1]))
        segments.append(codify(p[2]))
        segments += uri_segments_from_provenance(p[3])

    elif p[0] == 'partial':
        segments.append(hashlib.md5(json.dumps([p[1:3]]).encode()).hexdigest()[:8])
        segments.append(codify(p[3][0]))
        segments += uri_segments_from_provenance(p[3][1])
    elif isinstance(p, (list, tuple)):
        for e in p:
            segments += uri_segments_from_provenance(e)
    else:
        raise NotImplementedError


    logger.info(" [r]segment:[/] %s", segments)

    return segments


def uri_from_provenance(p):
    segments = uri_segments_from_provenance(p)

    if not re.match(r"^((ipynb|py)\+)?file://.*$", segments[0]):
        segments.insert(0, "file:///tmp/urivalue/")

    for i in range(1, len(segments)):
        segments[i] = re.sub(r"(ipynb|py\+)?(file|http|https)://", "", segments[i])

    for i in range(len(segments)):
        segments[i] = re.sub("@", "/", segments[i])

    return rdflib.URIRef(("/".join(segments)).rstrip("/"))
    



class URIFileFunction(URIFunction):
    """
    when URI actually represents a file which can be stored locally and loaded
    """

    def __init__(self, uri=None, value=None, provenance=None, verify_revision=True) -> None:
        super().__init__(uri, value, provenance)

        if value is None:
            self.load_func()
        else:
            self.write_to_uri(value)
            self._value = value

        if verify_revision:
            if (self.revision or "") != self.actual_revision_str:
                raise RuntimeError(f'unable to discover {self.uri} at requested revision "{self.revision}", have "{self.actual_revision_str}"')


    @classmethod
    def from_generic_uri(cls, uri):
        f = cls(uri, verify_revision=False)
        
        return cls(f.uri.split("@")[0] + "@" + f.actual_revision_str)
                

    @property
    def actual_revision_str(self):
        return ",".join([f"{k}={v}" for k, v in sorted(self.actual_revision_dict.items())])

    
    @property
    def actual_revision_dict(self):
        return {}


    def write_to_uri(self, value):
        logger.warning("asked to [red]write_to_uri[/] [b]%s[/] but this function has no persistent representation", self)
        

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



class URIPythonFunction(URIFileFunction, LocalPythonFunction):
    suffix="py"
    

    def __init__(self, uri=None, func=None, provenance=None, **kwargs) -> None:
        LocalPythonFunction.__init__(self, func)
        URIFileFunction.__init__(self, uri=uri, value=func, provenance=provenance, **kwargs)


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


    def __call__(self, *args: Any, **kwds: Any):
        logger.info("calling %s", self)
        lf = LocalPythonFunction.__call__(self, *args, **kwds)
        logger.info("LocalPythonFunction: %s", lf)
        f = URIPythonFunction(uri=None, func=lf.local_python_function, provenance=lf.provenance)
        logger.info("URIPythonFunction: %s", f)
        return f


class URIipynbFunction(URIPythonFunction):
    suffix = "ipynb"

    @property
    def actual_revision_dict(self):
        d = super().actual_revision_dict

        if self.version:
            d['oda_version'] = self.version
        
        return d


    def nba_to_oda_version(self, nba):
        G = rdflib.Graph()
        G.parse(data=nba.extra_ttl)        

        p_version = rdflib.URIRef("http://odahub.io/ontology#version")

        logger.debug('nba.extra_ttl for %s:\n %s', nba.nb_uri, nba.extra_ttl)

        v = list(G.objects(nba.nb_uri, p_version))

        if len(v) == 1:
            self.version = v[0]
            logger.debug("discovered version %s", self.version)
        elif len(v) > 1:
            raise NotImplementedError(f"discovered several notebook versions: {v}")
        else:
            self.version = None
            


    def load_func_from_local_file(self, path):
        nba = NotebookAdapter(path)
        
        self.parameters = nba.extract_parameters()

        logger.info("parameter definitions: %s", self.parameters)
        logger.info("output definitions: %s", nba.extract_output_declarations())        
        logger.info("function spot uri:\n %s", nba.nb_uri)
        logger.info("extra_ttl:\n %s", nba.extra_ttl)

        self.nba_to_oda_version(nba)
        logger.info("discovered function version %s", self.version)

        def local_python_function(*args, **kwargs):            
            if len(args) > 0:
                raise NotImplementedError(f"ipynb function can not consume positional args: {args}")

            nba = NotebookAdapter(path)
            print("nba", nba)
            nba.execute(kwargs, inplace=False)
            # nba.execute({}, inplace=getattr(self, 'inplace', False))
            output = nba.extract_output()
            with open(nba.output_notebook_fn) as f:
                output_nb = json.load(f)

            nba.remove_tmpdir()
            return {
                'output_nb': output_nb,
                'output_values': output
            }

        self.local_python_function = local_python_function


    @property
    def signature(self):
        return inspect.Signature(
            parameters=[inspect.Parameter(P['name'], kind=inspect.Parameter.KEYWORD_ONLY, default=P['value']) for p, P in self.parameters.items()]
        )

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



class URIValue(URIFileFunction, LocalValue):
    """
    nullary function returning value stored as byte content at URI
    """

    # cached = True

                
    def write_to_uri(self, value):
        if self.schema != 'file':
            raise NotImplementedError

        os.makedirs(Path(self.path).parent, exist_ok=True)

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
        logger.info("load from local file: %s", path)

        with open(path, "r") as f:
            self._value = json.load(f)


    def __repr__(self) -> str:
        return super().__repr__() + f"[uri: {self.uri} value: {repr_trim(self.value)}]"


    @property
    def constructor_args(self):
        return {**super().constructor_args, 'uri': self.uri}