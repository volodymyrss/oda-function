import inspect
import logging

from .. import LocalValue, LocalPythonFunction, Function, Executor

class LocalExecutor(Executor):
    def __call__(self, func: LocalPythonFunction) -> Function:
        if func.signature != inspect.Signature():
            raise RuntimeError(f"found non-0 signature: {func.signature}, please reduced function arguments before passing it to executors")
        
        r = LocalValue(func.local_python_function())

        self.note_execution(func, r)

        return r


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


