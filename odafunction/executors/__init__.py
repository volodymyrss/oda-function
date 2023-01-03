import hashlib
import inspect
import json
import logging
import os
import pathlib

from .. import LocalValue, LocalPythonFunction, Function, Executor


logger = logging.getLogger(__name__)

class LocalExecutor(Executor):
    def __call__(self, func: LocalPythonFunction) -> Function:
        if func.signature != inspect.Signature():
            raise RuntimeError(f"found non-0 signature: {func.signature}, please reduced function arguments before passing it to executors")
        
        r = LocalValue(func.local_python_function())

        self.note_execution(func, r)

        return r


class LocalCachingExecutor(LocalExecutor):
    caching = True

    def cache_path(self, func):
        uid = hashlib.md5(str(func._provenance).encode()).hexdigest()[:8]
        return pathlib.Path(os.getenv('HOME', './')) / f".cache/odafunction/{uid}.json"

    def __call__(self, func: LocalPythonFunction) -> Function:
        cp = self.cache_path(func)

        try:
            with open(cp) as f:
                r = LocalValue.from_f(f)

            logger.info("loaded from cache %s: %s", cp, r)
        except Exception as e:
            logger.info("can not load from cache %s: %s", cp, e)
            r = super().__call__(func)                        

            cp.parent.mkdir(parents=True, exist_ok=True)
            with open(cp, "w") as f:
                f.write(r.dumps())
            logger.info("stored to cache %s, %s", cp, r)
        
        return r



def iterate_subclasses(cls):
    yield cls

    for c in cls.__subclasses__():
        yield from iterate_subclasses(c)
        
    
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
                logging.info("executor %s spec %s", cls, spec)

                if not isinstance(func, spec.annotations['func']):
                    logging.info("executor %s does not fit: func: %s but executor annotation is %s", cls, func, spec.annotations['func'])
                elif not issubclass(result_type, spec.annotations.get('return')):
                    logging.info("executor %s does not fit: func result type %s but executor annotation %s", cls, result_type, spec.annotations.get('return'))
                elif not self._executor_selector(cls):
                    logging.info("executor %s rejected by the executor filter", cls)
                else:
                    logging.info("executor fits!")

                    # TODO: not only first!
                    if 'result_type' in spec.annotations:                        
                        return cls()(func, result_type)
                    else:
                        return cls()(func)
                
        raise RuntimeError("all executors gave up")




def default_execute_to_local_value(f):
    # only transform nullary function to local value
    if isinstance(f, Function):
        logger.info("default_execute: %s", f)
        if f.signature == inspect.Signature():
            return AnyExecutor()(f, LocalValue).value
        else:
            return f
    else:
        return f