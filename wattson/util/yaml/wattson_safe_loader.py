import sys

import numpy.core.multiarray
import yaml


class WattsonSafeLoader(yaml.SafeLoader):
    def construct_python_tuple(self, node):
        return tuple(self.construct_sequence(node))

    def construct_numpy_scalar(self, node):
        print(repr(node))
        sys.exit(0)
        return numpy.core.multiarray.scalar(node)


WattsonSafeLoader.add_constructor(u'tag:yaml.org,2002:python/tuple', WattsonSafeLoader.construct_python_tuple)
WattsonSafeLoader.add_constructor(
    u'tag:yaml.org,2002:python/object/apply:numpy.core.multiarray.scalar',
    WattsonSafeLoader.construct_numpy_scalar
)
