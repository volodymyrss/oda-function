import pytest

from odafunction import LocalPythonFunction, LocalValue, LocalExecutor, AnyExecutor, default_execute_to_local_value
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
        
    fc = FunctionCatalogKeyedLocalValuedAttrs()
    fc.add("examplefunc", URIPythonFunction("file://tests/test_data/filewithfunc.py::examplefunc"))

    print(fc.catalog)

    assert fc.examplefunc(1,2,3) == 6


def test_catalog():    
    # TODO: fetch all sorts of catalogs
    
    # raise NotImplementedError
    pass


def test_deep_function():
    add = URIPythonFunction("file://tests/test_data/filewithfunc.py::examplefunc")
    increment = LocalPythonFunction(lambda x:x+1)
    
    fg = add(increment(increment(increment(1))), 1, 2)

    assert AnyExecutor()(fg, LocalValue).value == 1+1+1+1 + 1 + 2

    print(fg)


def test_http():
    f = URIPythonFunction("https://raw.githubusercontent.com/oda-hub/oda_test_kit/master/test_image.py::get_scw_list")
    v = default_execute_to_local_value(f(ra_obj=82, dec_obj=22, radius=5, start_date="2011-11-11T11:11:11", end_date="2012-11-11T11:11:11"))
    assert len(v) == 149
    assert '115900920010.001' in v


def test_higher_order_function():    
    pass