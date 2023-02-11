import hashlib
import rdflib
import inspect
import json
import logging
import os
import pathlib
import traceback

from .. import LocalValue, LocalPythonFunction, Function, Executor
from ..func.urifunc import URIPythonFunction, URIValue, URIFunction
from ..utils import iterate_subclasses, repr_trim


logger = logging.getLogger(__name__)

# provenance derives in two ways: partially applying functions, and executing them (TODO: it's basically the same)


class LocalExecutor(Executor):
    output_value_class=LocalValue

    def __call__(self, func: LocalPythonFunction) -> LocalValue:
        if func.signature != inspect.Signature():
            raise RuntimeError(f"found non-0 signature: {func.signature}, please reduced function arguments before passing it to executors")
        
        logger.info("executor: %s running func: %s", self, func)
        v = func.local_python_function()
        logger.info("found value %s", repr_trim(v))
        ex = Executor()
        
        r = self.output_value_class(value=v, provenance=ex(func, type).provenance)
        logger.info("constructing output class %s as %s", self.output_value_class, r)

        self.note_execution(func, r)
        return r


class LocalURICachingExecutor(LocalExecutor):
    caching=True

    # executor only stores equivalences, not values

    @property
    def uri(self):
        return rdflib.URIRef(f"https://odahub.io/ontology#{self.__class__.__name__}")

    def __init__(self, memory_graph_path=None) -> None:
        super().__init__()

        if memory_graph_path is not None:
            self.memory_graph_path = memory_graph_path

        self.load_cache()

    
    @property
    def memory_graph_path(self):
        if not hasattr(self, '_memory_graph_path'):
            self._memory_graph_path = pathlib.Path(os.environ['HOME']) / f".cache/odafunction/{self.__class__.__name__}-memory-graph.ttl"

        return self._memory_graph_path

    @memory_graph_path.setter
    def memory_graph_path(self, value):
        self._memory_graph_path = pathlib.Path(value)


    def load_cache(self):
        self.memory_graph = rdflib.Graph()

        if self.memory_graph_path.exists():
            self.memory_graph.parse(open(str(self.memory_graph_path)), format="turtle")
            logger.info("loaded cache from %s; %s entries", self.memory_graph_path, len(list(self.memory_graph)))
        else:
            logger.info("initialized empty cache")


    def save_cache(self):
        self.memory_graph.serialize(open(str(self.memory_graph_path), "wb"), format="turtle")
        logger.info("stored cache to %s", self.memory_graph_path)

    
    def __call__(self, func: URIPythonFunction) -> URIValue:
        
        objects = list(self.memory_graph.objects(func.uri, self.uri))

        if len(objects) == 1:
            logger.info("memory has entry %s %s %s", func.uri, self.uri, objects[0])
            r = URIValue(uri=objects[0])
            logger.info("loaded from cache %s", r)
            
        elif len(objects) == 0:        
            logger.info("can not load from cache %s %s ?", func.uri, self.uri)            

            logger.info("will run %s", func)
            lv = super().__call__(func)
            r = URIValue(value=lv.value, provenance=lv.provenance)

            self.memory_graph.add((func.uri, self.uri, r.uri))

            self.save_cache()
            
        
        return r


class LocalURIExecutor(LocalExecutor):
    output_value_class=URIValue

    def __call__(self, func: LocalPythonFunction) -> URIValue:
        return super().__call__(func)



class AnyExecutor(Executor):

    def __init__(self, executor_selector=None) -> None:
        if executor_selector is None:
            self._executor_selector = lambda ex: True
        else:
            self._executor_selector = executor_selector

        super().__init__()

    def __call__(self, func: Function, result_type: type) -> Function:
        for cls in iterate_subclasses(Executor):
            if cls != self.__class__:
                spec = inspect.getfullargspec(cls.__call__)
                logging.info("for func %s result_type %s executor %s spec %s", func, result_type, cls, spec)

                if not isinstance(func, spec.annotations['func']):
                    logging.info("executor %s does not fit: func: %s but executor annotation is %s", cls, func, spec.annotations['func'])
                elif not issubclass(spec.annotations.get('return'), result_type):
                    logging.info("executor %s does not fit: func result type %s but executor annotation %s", cls, result_type, spec.annotations.get('return'))
                elif not self._executor_selector(cls):
                    logging.info("executor %s rejected by the executor filter", cls)
                else:
                    logging.info("executor %s fits!", cls)

                    # TODO: not only first!
                    if 'result_type' in spec.annotations:                        
                        return cls()(func, result_type)
                    else:
                        return cls()(func)
                
        raise RuntimeError("all executors gave up")


# TODO move somewhere
default_execute_to_value_cached = False

def default_execute_to_value(f, cached=None, valueclass: type=LocalValue):
    # only transform nullary function to local value

    if cached is None:
        cached = default_execute_to_value_cached
    
    if cached:
        f.cached = True
        selector = lambda ex: getattr(ex, 'caching', False )
    else:
        selector = lambda ex: True

    if isinstance(f, Function):
        logger.info("default_execute: %s", f)
        if f.signature == inspect.Signature():
            logger.info("nullary: default_execute proceeds to AnyExecutor")
            return AnyExecutor(executor_selector=selector)(f, valueclass).value
        else:
            logger.info("NOT nullary returning")
            return f
    else:
        return f