import hashlib
import inspect
import json
import logging
import os
import pathlib
import traceback

from .. import LocalValue, LocalPythonFunction, Function, Executor
from ..func.urifunc import URIValue
from ..utils import iterate_subclasses


logger = logging.getLogger(__name__)

# provenance derives in two ways: partially applying functions, and executing them (TODO: it's basically the same)

class LocalExecutor(Executor):
    output_value_class=LocalValue

    def __call__(self, func: LocalPythonFunction) -> LocalValue:
        if func.signature != inspect.Signature():
            raise RuntimeError(f"found non-0 signature: {func.signature}, please reduced function arguments before passing it to executors")
        
        r = self.output_value_class(value=func.local_python_function(), provenance=[(self,)] + func.provenance)

        self.note_execution(func, r)
        return r


class LocalCachingExecutor(LocalExecutor):
    caching = True

    def cache_path(self, func):
        uid = hashlib.md5(str(func._provenance).encode()).hexdigest()[:8]
        logger.info('uid %s from provenance %s', uid, func._provenance)
        return pathlib.Path(os.getenv('HOME', './')) / f".cache/odafunction/{uid}.json"


    def __call__(self, func: LocalPythonFunction) -> LocalValue:
        if func.cached:
            logger.info("function %s is caching", func)

            cp = self.cache_path(func)

            try:
                with open(cp) as f:
                    r = self.output_value_class.from_f(f)

                logger.info("loaded from cache %s: %s", cp, r)
            except Exception as e:
                logger.info("can not load from cache %s: %s", cp, e)
                logger.debug("%s", traceback.format_exc())
                r = super().__call__(func)

                cp.parent.mkdir(parents=True, exist_ok=True)
                with open(cp, "w") as f:
                    f.write(r.dumps())
                logger.info("stored to cache %s, %s", cp, r)
        else:
            logger.info("function %s is not caching", func)
            r = super().__call__(func)
        
        return r


class LocalURIExecutor(LocalExecutor):
    output_value_class=URIValue

    def __call__(self, func: LocalPythonFunction) -> URIValue:
        return super().__call__(func)



class LocalCachingURIExecutor(LocalCachingExecutor):
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




def default_execute_to_value(f, cached=False, valueclass: type=LocalValue):
    # only transform nullary function to local value
    
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