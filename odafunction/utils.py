
def iterate_subclasses(cls):
    yield cls

    for c in cls.__subclasses__():
        yield from iterate_subclasses(c)