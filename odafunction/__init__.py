from typing import Any, List
import inspect


class Function:
    # this is not call but argument substitution. only 0-functions can be executed by the executor
    def __call__(self, *args: Any, **kwds: Any):
        pass    

    @property
    def signature(self) -> inspect.Signature:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"[{self.__class__.__name__}: {self.signature}]"


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


# 
# .....
# 

class LocalPythonFunction(Function):
    def __init__(self, local_python_function) -> None:
        self.local_python_function = local_python_function
        super().__init__()

    @property
    def signature(self):
        return inspect.signature(self.local_python_function)


    def __call__(self, *args: Any, **kwds: Any):
        ba = self.signature.bind(*args, **kwds)
        return LocalPythonFunction(lambda: self.local_python_function(*ba.args, **ba.kwargs))


class LocalValue(Function):
    # a particular, largely artificial, kind of function is the one whos value can be retrieved locally
    def __init__(self, value) -> None:        
        self._value = value

    @property
    def value(self):
        return self._value


class LocalExecutor(Executor):
    def __call__(self, func: LocalPythonFunction) -> Function:
        if func.signature != inspect.Signature():
            raise RuntimeError(f"found non-0 signature: {func.signature}, please reduced function arguments before passing it to executors")

        return LocalValue(func.local_python_function())