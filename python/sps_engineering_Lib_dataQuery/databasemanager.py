import os

import pandas as pd
import numpy as np
import psycopg2
from matplotlib.dates import num2date

import sps_engineering_Lib_dataQuery as dataQuery
from sps_engineering_Lib_dataQuery.dates import astro2num, str2astro


class PfsData(pd.DataFrame):
    def __init__(self, data, columns):
        pd.DataFrame.__init__(self, data=data, columns=columns)

    @property
    def strdate(self):
        return num2date(self['tai']).isoformat()[:19]

    def __getitem__(self, key):
        vals = pd.DataFrame.__getitem__(self, key)

        return vals[0] if len(vals) == 1 else vals


class DatabaseManager(object):
    def __init__(self, ip, port, dbname='archiver'):

        self.ip = ip
        self.port = port
        self.dbname = dbname

        self.conn = False
        self.alarmPath = '%s/alarm/' % os.path.dirname(dataQuery.__file__)
        self.configPath = '%s/config/' % os.path.dirname(dataQuery.__file__)

    def init(self):
        self.nq = 0
        prop = "dbname='%s' user='pfs' host='%s' port='%s'" % (self.dbname, self.ip, self.port)
        conn = psycopg2.connect(prop)
        self.conn = conn

        return conn

    def sqlRequest(self, table, cols, where='', order='', limit=''):
        if self.nq > 1000:
            self.close()

        conn = self.conn if self.conn else self.init()
        cursor = conn.cursor()

        sqlQuery = """select %s from %s %s %s %s """ % (cols, table, where, order, limit)

        cursor.execute(sqlQuery)
        self.nq += 1

        return np.array(cursor.fetchall())

    def pfsdata(self, table, cols=False, where='', order='', limit='', convert=True):
        joinTable = 'reply_raw inner join %s on %s.raw_id=reply_raw.id' % (table, table)
        allCols = 'id,tai,%s' % cols if cols else 'id,tai'
        rawData = self.sqlRequest(table=joinTable,
                                  cols=allCols,
                                  where=where,
                                  order=order,
                                  limit=limit)

        if convert:
            rawData[:, 1] = astro2num(rawData[:, 1])

        return PfsData(data=rawData, columns=allCols.split(','))

    def dataBetween(self, table, cols, start, end=False):
        start = 'tai>%.2f' % str2astro(start)
        end = 'and tai<%.2f' % str2astro(end) if end else ''
        where = 'where (%s %s)' % (start, end)

        [[startId]] = self.sqlRequest('reply_raw', 'id', where='where %s' % start, limit='limit 1')
        [[firstId]] = self.sqlRequest(table, 'raw_id', where='where raw_id>%i' % startId, limit='limit 1')

        return self.pfsdata(table, cols, where=where, order='order by id asc')

    def last(self, table, cols=False, where='', order='order by raw_id desc', limit='limit 1'):

        return self.pfsdata(table, cols=cols, where=where, order=order, limit=limit)

    def getFirstId(self, table, datenum):
        [[startId]] = self.sqlRequest('reply_raw', 'id', where='where tai>%.2f' % datenum, limit='limit 1')
        [[firstId]] = self.sqlRequest(table, 'raw_id', where='where raw_id>%i' % startId, limit='limit 1')

        return firstId

    def getLastId(self, table, datenum):
        [[startId]] = self.sqlRequest('reply_raw', 'id', where='where tai<.2f' % datenum, limit='limit 1')
        [[lastId]] = self.sqlRequest(table, 'raw_id', where='where raw_id<%i' % startId, limit='limit 1')

        return lastId

    def allTables(self):
        cols = 'table_name'
        table = 'information_schema.tables'
        where = "where table_schema='public'"

        ignore = ['reply_raw', 'reply_hdr', 'actors', 'cmds', 'hub']

        array = self.sqlRequest(table=table, cols=cols, where=where)
        allTable = {}
        for table in array:
            actor = table[0].split('__')[0]
            if actor in allTable.keys():
                allTable[actor].append(table[0])
            elif actor not in ignore:
                allTable[actor] = [table[0]]

        return allTable

    def allColumns(self, tablename):
        cols = 'column_name'
        table = 'information_schema.columns'
        where = "where (table_schema='public' AND table_name='%s' and data_type !='text' and column_name!='raw_id')" % tablename
        array = self.sqlRequest(table=table, cols=cols, where=where)

        allCols = [col[0] for col in array]
        if allCols:
            return allCols
        else:
            raise ValueError('No columns')

    def close(self):
        try:
            self.conn.close()
        except:
            pass
        self.conn = False
