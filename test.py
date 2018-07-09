import doctest
import unittest

from gmaildump import gmailhistory

def suitefn():
    suite = unittest.TestSuite()
    suite.addTests(doctest.DocTestSuite(gmailhistory))
    return suite

if __name__ == "__main__":
    doctest.testmod(gmailhistory)
