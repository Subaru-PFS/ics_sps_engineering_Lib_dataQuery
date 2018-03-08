import os
import datetime as dt
import pickle
import time
import random

import numpy as np

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import sps_engineering_Lib_dataQuery as dataQuery
from sps_engineering_Lib_dataQuery.dates import all2num

confpath = '%s/config/' % os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..'))
alarmpath = '%s/alarm/' % os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..'))


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


class CurveConf(object):
    def __init__(self, deviceConf, ind):
        object.__init__(self)
        self.tablename = deviceConf.tablename
        self.deviceLabel = deviceConf.deviceLabel
        self.key = deviceConf.keys[ind]
        self.type = deviceConf.types[ind]
        self.label = deviceConf.labels[ind]
        self.unit = deviceConf.units[ind]
        self.lbound = deviceConf.lbounds[ind]
        self.ubound = deviceConf.ubounds[ind]
        self.ylabel = deviceConf.ylabels[ind]
        self.trange = deviceConf.ranges[ind]

    @property
    def curveLabel(self):
        return "%s_%s" % (self.deviceLabel, self.label)


class DevConf(object):
    def __init__(self, tablename, keys, types, lbounds, ubounds, units, ylabels, ranges, labels=None, deviceLabel=None,
                 botcmd=None):
        object.__init__(self)

        tablename = tablename.strip()
        labels = labels if labels is not None else keys
        deviceLabel = deviceLabel if deviceLabel is not None else (tablename.split('__')[1]).capitalize()
        botcmd = botcmd if botcmd is not None else (tablename.split('__')[1]).lower()

        keys = self.cleanSplit(keys)
        types = self.cleanSplit(types)
        lbounds = self.cleanSplit(lbounds)
        ubounds = self.cleanSplit(ubounds)
        units = self.cleanSplit(units)
        ylabels = self.cleanSplit(ylabels)
        ranges = self.cleanSplit(ranges)
        labels = self.cleanSplit(labels)

        self.tablename = tablename
        self.keys = keys
        self.types = types
        self.lbounds = lbounds
        self.ubounds = ubounds
        self.units = units
        self.ylabels = ylabels
        self.ranges = ranges
        self.labels = labels
        self.deviceLabel = deviceLabel.strip()
        self.botcmd = botcmd.strip()

    def cleanSplit(self, var):
        return [c.strip() for c in var.split(',')]

    def curveConf(self, ind):
        return CurveConf(self, ind=ind)


def getConfigParser(date=0.):
    datenum = all2num(date)
    configFiles = []

    all_file = [f for f in next(os.walk(confpath))[-1] if '.cfg' in f]
    for f in all_file:
        config = ConfigParser()
        config.read_file(open('%s/%s' % (confpath, f)))
        try:
            date = config.get('config_date', 'date')
            configFiles.append((f, datenum - all2num(dt.datetime.strptime(date, "%Y-%m-%d"))))
        except configparser.NoSectionError:
            pass

    configFiles.sort(key=lambda tup: tup[1])

    deltas = np.array([delta for fname, delta in configFiles])
    ind = np.argmax(deltas >= 0) if datenum else 0

    file = configFiles[ind][0]
    config = ConfigParser()
    config.read_file(open('%s/%s' % (confpath, file)))

    return config


def loadConf(date=0):
    datatype = ConfigParser()
    datatype.read('%s/datatype.cfg' % confpath)
    datatype = datatype._sections
    allConfig = []

    config = getConfigParser(date=date)
    tablenames = [table for table in config.sections() if table != 'config_date']

    for tablename in tablenames:
        keys = config.get(tablename, 'key')
        types = config.get(tablename, 'type')
        lbounds = config.get(tablename, 'lower_bound')
        ubounds = config.get(tablename, 'upper_bound')
        units = ','.join([datatype[typ.strip()]['unit'] for typ in types.split(',')])
        ylabels = ','.join([datatype[typ.strip()]['ylabel'] for typ in types.split(',')])
        ranges = ','.join([datatype[typ.strip()]['range'] for typ in types.split(',')])
        labels = config.get(tablename, 'label') if 'label' in config.options(tablename) else None
        deviceLabel = config.get(tablename, 'label_device') if 'label_device' in config.options(tablename) else None
        botcmd = config.get(tablename, 'bot_cmd') if 'bot_cmd' in config.options(tablename) else None

        allConfig.append(DevConf(tablename=tablename,
                                 keys=keys,
                                 types=types,
                                 lbounds=lbounds,
                                 ubounds=ubounds,
                                 units=units,
                                 ylabels=ylabels,
                                 ranges=ranges,
                                 labels=labels,
                                 deviceLabel=deviceLabel,
                                 botcmd=botcmd))

    return allConfig


def loadAlarm():
    listAlarm = []
    modes = readMode()

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
    return unPickle('%s/opmode.pickle' % alarmpath)


def readState():
    return unPickle('%s/state.pickle' % alarmpath)


def readTimeout():
    return unPickle('%s/timeout.pickle' % alarmpath)


def writeMode(mode):
    doPickle('%s/opmode.pickle' % alarmpath, mode)


def writeState(mode):
    doPickle('%s/state.pickle' % alarmpath, mode)


def writeTimeout(mode):
    doPickle('%s/timeout.pickle' % alarmpath, mode)


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
        pickler = pickle.Pickler(thisFile, protocol=2)
        pickler.dump(var)
