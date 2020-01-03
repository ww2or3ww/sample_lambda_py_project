from __future__ import division, absolute_import, print_function

import inspect
import sys
import pytest

from numpy.core import arange
from numpy.testing import assert_, assert_equal, assert_raises_regex
from numpy.lib import deprecate
import numpy.lib.utils as utils

if sys.version_info[0] >= 3:
    from io import StringIO
else:
    from StringIO import StringIO


@pytest.mark.skipif(sys.flags.optimize == 2, reason="Python running -OO")
def test_lookfor():
    out = StringIO()
    utils.lookfor('eigenvalue', module='numpy', output=out,
                  import_modules=False)
    out = out.getvalue()
    assert_('numpy.linalg.eig' in out)


@deprecate
def old_func(self, x):
    return x


@deprecate(message="Rather use new_func2")
def old_func2(self, x):
    return x


def old_func3(self, x):
    return x
new_func3 = deprecate(old_func3, old_name="old_func3", new_name="new_func3")


def old_func4(self, x):
    """Summary.

    Further info.
    """
    return x
new_func4 = deprecate(old_func4)


def old_func5(self, x):
    """Summary.

        Bizarre indentation.
    """
    return x
new_func5 = deprecate(old_func5)


def old_func6(self, x):
    """
    Also in PEP-257.
    """
    return x
new_func6 = deprecate(old_func6)


def test_deprecate_decorator():
    assert_('deprecated' in old_func.__doc__)


def test_deprecate_decorator_message():
    assert_('Rather use new_func2' in old_func2.__doc__)


def test_deprecate_fn():
    assert_('old_func3' in new_func3.__doc__)
    assert_('new_func3' in new_func3.__doc__)


@pytest.mark.skipif(sys.flags.optimize == 2, reason="-OO discards docstrings")
def test_deprecate_help_indentation():
    _compare_docs(old_func4, new_func4)
    _compare_docs(old_func5, new_func5)
    _compare_docs(old_func6, new_func6)


def _compare_docs(old_func, new_func):
    old_doc = inspect.getdoc(old_func)
    new_doc = inspect.getdoc(new_func)
    index = new_doc.index('\n\n') + 2
    assert_equal(new_doc[index:], old_doc)


@pytest.mark.skipif(sys.flags.optimize == 2, reason="-OO discards docstrings")
def test_deprecate_preserve_whitespace():
    assert_('\n        Bizarre' in new_func5.__doc__)


def test_safe_eval_nameconstant():
    # Test if safe_eval supports Python 3.4 _ast.NameConstant
    utils.safe_eval('None')


class TestByteBounds(object):

    def test_byte_bounds(self):
        # pointer difference matches size * itemsize
        # due to contiguity
        a = arange(12).reshape(3, 4)
        low, high = utils.byte_bounds(a)
        assert_equal(high - low, a.size * a.itemsize)

    def test_unusual_order_positive_stride(self):
        a = arange(12).reshape(3, 4)
        b = a.T
        low, high = utils.byte_bounds(b)
        assert_equal(high - low, b.size * b.itemsize)

    def test_unusual_order_negative_stride(self):
        a = arange(12).reshape(3, 4)
        b = a.T[::-1]
        low, high = utils.byte_bounds(b)
        assert_equal(high - low, b.size * b.itemsize)

    def test_strided(self):
        a = arange(12)
        b = a[::2]
        low, high = utils.byte_bounds(b)
        # the largest pointer address is lost (even numbers only in the
        # stride), and compensate addresses for striding by 2
        assert_equal(high - low, b.size * 2 * b.itemsize - b.itemsize)


def test_assert_raises_regex_context_manager():
    with assert_raises_regex(ValueError, 'no deprecation warning'):
        raise ValueError('no deprecation warning')
