from . import FunctionCatalog, Function, LocalValue
from .executors import AnyExecutor


class FunctionCatalogKeyedLocalValued(FunctionCatalog):
    def __init__(self) -> None:
        self.catalog = {}

    def add(self, key, func):
        self.catalog[key] = func

    def find(self, key):
        return self.catalog[key]
    

class FunctionCatalogKeyedLocalValuedAttrs(FunctionCatalogKeyedLocalValued):
    def __getattr__(self, __name: str) -> callable:
        if __name in self.catalog:
            func = self.catalog[__name]

            def f(*args, **kwds):
                f0 = func(*args, **kwds)
                return AnyExecutor()(f0, LocalValue).value

            return f

        return super().__getattr__(__name)    


# class FunctionCatalogAsItems(FunctionCatalogKeyed):
#     def __getitem__(self, key):    
#         return self.catalog[key]
            