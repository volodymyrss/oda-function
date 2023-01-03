import inspect
import logging

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
    def __call__(self, func: LocalPythonFunction) -> Function:
        r = super().__call__(func)
        return r


class AnyExecutor(Executor):

    def __init__(self, executor_selector=None) -> None:
        if executor_selector is None:
            self._executor_selector = lambda ex: True
        else:
            self._executor_selector = executor_selector

        super().__init__()

    def __call__(self, func: Function, result_type: type) -> Function:
        for cls in Executor.__subclasses__():
            if cls != self.__class__:
                spec = inspect.getfullargspec(cls.__call__)
                logging.info("executor %s spec %s", cls, spec)

                if not isinstance(func, spec.annotations['func']):
                    logging.info("executor %s does not fit: func: %s but executor annotation is %s", cls, func, spec.annotations['func'])
                elif not issubclass(result_type, spec.annotations.get('return')):
                    logging.info("executor %s does not fit: func result type %s but executor annotation %s", cls, result_type, spec.annotations.get('return'))
                elif not self._executor_selector(cls):
                    logging.info("executor %s rejected by the executor filter")
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