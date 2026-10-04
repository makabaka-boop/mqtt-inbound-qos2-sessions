#!/usr/bin/env python3
"""测试入口：python tests/run_tests.py [-v] [测试名...]"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        names = sys.argv[1:]
        suite = unittest.TestSuite()
        for name in names:
            suite.addTests(loader.loadTestsFromName(name))
    else:
        suite = loader.loadTestsFromName("test_mqtt_receiver")

    runner = unittest.TextTestRunner(verbosity=2 if "-v" in sys.argv else 1)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
