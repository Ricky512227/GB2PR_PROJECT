import importlib

def test_import_package():
    gb2pr = importlib.import_module('gb2pr')
    assert hasattr(gb2pr, 'logger')
    assert hasattr(gb2pr, 'executeCmd')


def test_import_submodules():
    mod = importlib.import_module('gb2pr.CommonUtlity')
    assert hasattr(mod, 'executeCmd')
    mod2 = importlib.import_module('gb2pr.recordCount')
    assert hasattr(mod2, 'feedRunner')
