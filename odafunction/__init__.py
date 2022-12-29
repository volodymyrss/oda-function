from typing import Any, List
import inspect
import typing

import logging

logger = logging.getLogger(__name__)

# sometimes, as the progress is made, code develops its own autonomous logic. it's reconciliation, harmony, and creation

rdf_prefix = "<http://odahub.io/ontology/odafunction#>"


class Function:

    def __init__(self, provenance=None) -> None:
        self._provenance = provenance

    @property
    def provenance(self):
        return getattr(self, '_provenance', None)

    # this is not call but argument substitution. only 0-functions can be executed by the executor
    def __call__(self, *args: Any, **kwds: Any):
        pass    

    @property
    def signature(self) -> inspect.Signature:
        raise NotImplementedError

    def __repr__(self) -> str:
        try:
            sig = self.signature
        except NotImplementedError:
            sig = None

        r = f"[{self.__class__.__name__}]"
        if sig:
            r += f"[{sig}]"
        
        if self.provenance:
            r += f"[prov: {self.provenance}]"

        return r


class FunctionCatalog:
    def __init__(self) -> None:
        self.functions = []

    def find(self, *args, **kwds) -> List[Function]:
        return self.functions

    def add(self, func):
        self.functions.append(func)


class Executor:
    # executor transforms 0-function to another 0-function
    # typically, execution transforms "data" to other "data", but both of these "data" can only be fetched with a request (i.e. another 0-function)
    def __call__(self, func: Function, result_type: type) -> Function:
        return func

    def note_execution(self, func, r):
        logger.info("execution derives equivalence %s == %s", func, r)

    def __repr__(self) -> str:
        return f"[{self.__class__.__name__}: {inspect.signature(self.__call__)}]"


class AnyExecutor(Executor):
    def __call__(self, func: Function, result_type: type) -> Function:
        for cls in Executor.__subclasses__():
            if cls != self.__class__:
                spec = inspect.getfullargspec(cls.__call__)
                logging.info("executor %s spec %s", cls, spec)

                if not isinstance(func, spec.annotations['func']):
                    logging.info("executor %s does not fit: func: %s but executor annotation is %s", cls, func, spec.annotations['func'])
                elif not issubclass(result_type, spec.annotations.get('return')):
                    logging.info("executor %s does not fit: func result type %s but executor annotation %s", cls, result_type, spec.annotations.get('return'))
                else:
                    logging.info("executor fits!")
                    # TODO: not only first!
                    if 'result_type' in spec.annotations:                        
                        return cls()(func, result_type)
                    else:
                        return cls()(func)
                
        raise RuntimeError("all executors gave up")



# 
# local python
# 


def default_execute_to_local_value(f):
    if isinstance(f, Function):
        logger.info("default_execute: %s", f)
        return AnyExecutor()(f, LocalValue).value
    else:
        return f

class LocalPythonFunction(Function):
    def __init__(self, local_python_function, provenance=None) -> None:
        self.local_python_function = local_python_function
        super().__init__(provenance=provenance)

    @property
    def signature(self):
        return inspect.signature(self.local_python_function)    

    def __call__(self, *args: Any, **kwds: Any):
        ba = self.signature.bind(*args, **kwds)        
        provenance = (self.provenance or []) + [(self, args, kwds)]

        def f():
            # TODO: these assumptions about executor are not universal
            args = [default_execute_to_local_value(a) for a in ba.args]
            kwargs = {k: default_execute_to_local_value(v) for k, v in ba.kwargs.items()}
            return self.local_python_function(*args, **kwargs)

        return LocalPythonFunction(f, provenance=provenance)


class LocalValue(Function):
    # a particular, largely artificial, kind of function is the one whos value can be retrieved locally
    def __init__(self, value) -> None:        
        self._value = value

    @property
    def value(self):
        return self._value


    def __repr__(self) -> str:
        return super().__repr__() + f"[value: {self.value}]"


class LocalExecutor(Executor):
    def __call__(self, func: LocalPythonFunction) -> Function:
        if func.signature != inspect.Signature():
            raise RuntimeError(f"found non-0 signature: {func.signature}, please reduced function arguments before passing it to executors")
        
        r = LocalValue(func.local_python_function())

        self.note_execution(func, r)

        return r


