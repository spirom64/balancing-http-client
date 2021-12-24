from functools import partial
import os.path
import unittest

import pycodestyle

from tests import ROOT


class TestPycodestyle(unittest.TestCase):
    CHECKED_PATHS = ('http_client', 'tests', 'setup.py')

    def test_pycodestyle(self):
        style_guide = pycodestyle.StyleGuide(
            show_pep8=False,
            show_source=True,
            max_line_length=120,
            ignore=['E731', 'W504']
        )
        result = style_guide.check_files(map(partial(os.path.join, ROOT), TestPycodestyle.CHECKED_PATHS))
        self.assertEqual(result.total_errors, 0, 'Pycodestyle found code style errors or warnings')
