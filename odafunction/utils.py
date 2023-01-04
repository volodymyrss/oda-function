
def iterate_subclasses(cls):
    yield cls

    for c in cls.__subclasses__():
        yield from iterate_subclasses(c)


def repr_trim(o, lim=30):
    s = str(o)    
    if len(s) > lim:
        s = f"{s[:lim]}...({len(s)})"
    
    return s
