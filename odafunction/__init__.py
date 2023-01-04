import json
from typing import Any, List
import inspect
import typing

import logging

logger = logging.getLogger(__name__)

# sometimes, as the progress is made, code develops its own autonomous logic. it's reconciliation, harmony, and creation

rdf_prefix = "<http://odahub.io/ontology/odafunction#>"



        

class Function:
    cached = False

    def __init__(self, provenance=None) -> None:
        self._provenance = provenance

    @property
    def provenance(self):
        return getattr(self, '_provenance', None)

    # this is not call but argument substitution. only nullary functions can be executed by the executor
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
    # executor transforms nullary function to another nullary function
    # typically, execution transforms "data" to other "data", but both of these "data" can only be fetched with a request (i.e. another nullary function)
    def __call__(self, func: Function, result_type: type) -> Function:
        return func

    def note_execution(self, func, r):
        logger.info("execution derives equivalence: \n   %s\n   =(%s)=   %s", func, self, r)

    def __repr__(self) -> str:
        return f"[{self.__class__.__name__}: {inspect.signature(self.__call__)}]"



# 
# local python
# 


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
            from odafunction.executors import default_execute_to_local_value
            args = [default_execute_to_local_value(a) for a in ba.args]
            kwargs = {k: default_execute_to_local_value(v) for k, v in ba.kwargs.items()}
            return self.local_python_function(*args, **kwargs)

        return LocalPythonFunction(f, provenance=provenance)


class LocalValue(Function):
    cached = True

    # a particular, largely artificial, kind of function is the one whos value can be retrieved locally
    def __init__(self, value) -> None:        
        self._value = value

    @property
    def value(self):
        return self._value

    def __repr__(self) -> str:
        return super().__repr__() + f"[value: {self.value}]"

    def dumps(self):
        return json.dumps({'value': self.value, 'class': self.__class__.__name__})

    @classmethod    
    def from_s(cls, s):
        return cls(json.loads(s)['value'])

    @classmethod    
    def from_f(cls, f):
        return cls(json.load(f)['value'])

