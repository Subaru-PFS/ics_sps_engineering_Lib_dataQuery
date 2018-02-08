import os
import datetime as dt
import pickle
import time
import random
import numpy as np
from matplotlib.dates import date2num

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import sps_engineering_Lib_dataQuery.config as aitConf
import sps_engineering_Lib_dataQuery.alarm as aitAlarm

confpath = os.path.dirname(aitConf.__file__)
alarmpath = os.path.dirname(aitAlarm.__file__)


class ConfigParser(configparser.ConfigParser):
    def __init__(self, *args, **kwargs):
        configparser.ConfigParser.__init__(self, *args, **kwargs)
        if not hasattr(self, 'read_file'):
            self.read_file = self.readfp


class Alarm(object):
    def __init__(self, mode, label, tablename, key, lbound, ubound):
        self.mode = mode
        self.label = label
        self.tablename = tablename
        self.key = key
        self.lbound = lbound
        self.ubound = ubound


class DevConf(object):
    def __init__(self, tablename, keys, types, lbounds, ubounds, units, labels=None, labelDev=None,
                 botcmd=None):
        object.__init__(self)
        self.tablename = tablename
        self.keys = keys
        self.types = types
        self.lbounds = lbounds
        self.ubounds = ubounds
        self.units = units
        self.labels = labels if labels is not None else keys
        self.labelDev = labelDev if labelDev is not None else (tablename.split('__')[1]).capitalize()
        self.botcmd = botcmd if botcmd is not None else (tablename.split('__')[1]).lower()


def getConfigParser(datenum=0):
    configFiles = []

    all_file = [f for f in next(os.walk(confpath))[-1] if '.cfg' in f]
    for f in all_file:
        config = ConfigParser()
        config.read_file(open('%s/%s' % (confpath, f)))
        try:
            date = config.get('config_date', 'date')
            configFiles.append((f, datenum - date2num(dt.datetime.strptime(date, "%Y-%m-%d"))))
        except configparser.NoSectionError:
            pass

    configFiles.sort(key=lambda tup: tup[1])

    deltas = np.array([delta for fname, delta in configFiles])
    ind = np.argmax(deltas >= 0) if datenum else 0

    file = configFiles[ind][0]
    config = ConfigParser()
    config.read_file(open('%s/%s' % (confpath, file)))

    return config


def loadConf(datenum=0):
    datatype = ConfigParser()
    datatype.read('%s/datatype.cfg' % confpath)
    datatype = datatype._sections
    allConfig = []

    config = getConfigParser(datenum=datenum)
    tablenames = [table for table in config.sections() if table != 'config_date']

    for tablename in tablenames:
        keys = config.get(tablename, 'key')
        types = config.get(tablename, 'type')
        lbounds = config.get(tablename, 'lower_bound')
        ubounds = config.get(tablename, 'upper_bound')
        units = ','.join([datatype[typ.strip()]['unit'] for typ in types.split(',')])
        labels = config.get(tablename, 'label') if 'label' in config.options(tablename) else None
        labelDev = config.get(tablename, 'label_device') if 'label_device' in config.options(tablename) else None
        botcmd = config.get(tablename, 'bot_cmd') if 'bot_cmd' in config.options(tablename)  else None

        allConfig.append(DevConf(tablename=tablename,
                                 keys=keys,
                                 types=types,
                                 lbounds=lbounds,
                                 ubounds=ubounds,
                                 units=units,
                                 labels=labels,
                                 labelDev=labelDev,
                                 botcmd=botcmd))

    return allConfig


def loadAlarm():
    listAlarm = []
    modes = unPickle('%s/opmode.pickle' % alarmpath)

    for actor, mode in list(modes.items()):
        config = ConfigParser()
        config.read_file(open('%s/%s.cfg' % (alarmpath, mode)))
        sections = [a for a in config.sections() if actor in config.get(a, 'tablename')]

        for label in sections:
            listAlarm.append(Alarm(mode=mode,
                                   label=label,
                                   tablename=config.get(label, 'tablename'),
                                   key=config.get(label, 'key'),
                                   lbound=config.get(label, 'lower_bound'),
                                   ubound=config.get(label, 'upper_bound')))

    return listAlarm

def readMode():
    return unPickle('%s/opmode.pickle'%alarmpath)

def readState():
    return unPickle('%s/state.pickle'%alarmpath)

def readTimeout():
    return unPickle('%s/timeout.pickle'%alarmpath)

def writeMode(mode):
    doPickle('%s/opmode.pickle'%alarmpath, mode)

def writeState(mode):
    doPickle('%s/state.pickle'%alarmpath, mode)
    
def writeTimeout(mode):
    doPickle('%s/timeout.pickle'%alarmpath, mode)

def unPickle(filepath):
    try:
        with open(filepath, 'rb') as thisFile:
            unpickler = pickle.Unpickler(thisFile)
            return unpickler.load()
    except EOFError:
        time.sleep(0.1 + random.random())
        return unPickle(filepath=filepath)

def doPickle(filepath, var):
        with open(filepath, 'wb') as thisFile:
            pickler = pickle.Pickler(thisFile)
            pickler.dump(var)