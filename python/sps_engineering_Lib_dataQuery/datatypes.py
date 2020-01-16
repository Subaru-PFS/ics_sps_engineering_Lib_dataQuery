import os

import sps_engineering_Lib_dataQuery as dataQuery
import yaml

confpath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'config'))

with open(os.path.join(confpath, 'datatypes.yaml'), 'r') as cfgFile:
    datatypes = yaml.load(cfgFile)


class Datatypes(object):
    @staticmethod
    def types(types):
        return [t.strip() if t.strip() in datatypes.keys() else 'none' for t in types.split(',')]

    @staticmethod
    def getField(types, field):
        types = Datatypes.types(types)
        return ','.join([datatypes[dtype][field] for dtype in types])

    @staticmethod
    def getLabel(types):
        types = Datatypes.types(types)
        return ','.join([Datatypes.constructLabel(dtype) for dtype in types])

    @staticmethod
    def constructLabel(dtype):
        datatype = datatypes[dtype]
        ylabel = datatype['ylabel']
        return f'{dtype.capitalize()} ({datatype["unit"]})' if ylabel == 'auto' else ylabel
