"""Test runner, executed inside the spark container by `make test` (spark-submit resolves the Delta package).

Plain unittest on purpose: nothing extra to install in the image.
Usage: make test [TEST=test_velocity]   (TEST = module name filter)
"""
import sys
import unittest

for p in ("/jobs", "/producer", "/tests"):
    if p not in sys.path:
        sys.path.insert(0, p)

pattern = "%s*.py" % (sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else "test_")
suite = unittest.defaultTestLoader.discover("/tests", pattern=pattern)
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
