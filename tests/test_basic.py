import pytest

from odafunction import LocalPythonFunction, LocalValue, LocalExecutor, AnyExecutor
from odafunction.catalogviews import FunctionCatalogKeyedLocalValuedAttrs
from odafunction.urifunc import URIPythonFunction

def test_local_function():
    
    f = LocalPythonFunction(lambda x, y, z=1:(x+y+z))

    # fc.add(f)

    # assert fc.find() == [f]

    with pytest.raises(RuntimeError):
        LocalExecutor()(f)

    f0 = f(1,2,z=3)

    lv = LocalExecutor()(f0)

    assert isinstance(lv, LocalValue)
    assert lv.value == 6


# def test_http_function():
#     pass


def test_any_executor():
    f = LocalPythonFunction(lambda x, y, z=1:(x+y+z))

    assert AnyExecutor()(f(1, 2, 3), LocalValue).value == 6


def test_urifunc():
    f = URIPythonFunction("file://tests/test_data/filewithfunc.py::examplefunc")
        
    assert AnyExecutor()(f(1, 2, 3), LocalValue).value == 6


def test_localvalued():    

    f = URIPythonFunction("file://tests/test_data/filewithfunc.py::examplefunc")
        
    fc = FunctionCatalogKeyedLocalValuedAttrs()
    fc.add("examplefunc", f)

    assert fc.examplefunc(1,2,3) == 6