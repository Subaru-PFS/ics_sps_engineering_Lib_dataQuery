import os
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime as dt

import sps_engineering_Lib_dataQuery as dataQuery

from matplotlib.dates import num2date, date2num
from sps_engineering_Lib_dataQuery.confighandler import DummyConf, buildPfsConf, loadAlarms
from sps_engineering_Lib_dataQuery.dates import astro2num, str2astro, date2astro


class PfsData(pd.DataFrame):
    def __init__(self, data, columns):
        pd.DataFrame.__init__(self, data=data, columns=columns)

    @property
    def strdate(self):
        return num2date(self['tai']).isoformat()[:19]


class OneData(PfsData):
    def __init__(self, data, columns):
        PfsData.__init__(self, data=data, columns=columns)

    def __getitem__(self, key):
        vals = pd.DataFrame.__getitem__(self, key)
        return vals[0] if len(vals) == 1 else vals


class DatabaseManager(object):
    def __init__(self, ip, port, dbname='archiver'):

        self.ip = ip
        self.port = port
        self.dbname = dbname

        self.conn = False
        self.alarmPath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'alarm'))
        self.configPath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'config'))

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

        try:
            cursor.execute(sqlQuery)
            self.nq += 1
            return np.array(cursor.fetchall())

        except psycopg2.InternalError:
            self.close()

    def pfsdata(self, table, cols=False, where='', order='', limit='', convert=True, Obj=PfsData):
        joinTable = 'reply_raw inner join %s on %s.raw_id=reply_raw.id' % (table, table)
        allCols = 'id,tai,%s' % cols if cols else 'id,tai'
        rawData = self.sqlRequest(table=joinTable,
                                  cols=allCols,
                                  where=where,
                                  order=order,
                                  limit=limit)
        if not rawData.size:
            raise ValueError('no data')

        if convert:
            rawData[:, 1] = astro2num(rawData[:, 1])

        df = pd.DataFrame(data=rawData, columns=allCols.split(','))
        df.dropna(inplace=True)

        data = df.as_matrix().astype('float64')
        if not data.size:
            raise ValueError('no data')

        return Obj(data=data, columns=df.columns)

    def dataBetween(self, table, cols, start, end=False, raw_id=False):
        if not raw_id:
            closestId = self.closestId(table=table, date=start)
            start = 'tai>%.2f' % str2astro(start)
            end = 'and tai<%.2f' % str2astro(end) if end else ''
        else:
            start = 'id>%i' % start
            end = 'and id<%i' % end if end else ''

        where = 'where (%s %s)' % (start, end)

        return self.pfsdata(table, cols, where=where, order='order by id asc')

    def last(self, table, cols=False, where='', order='order by raw_id desc', limit='limit 1'):

        return self.pfsdata(table, cols=cols, where=where, order=order, limit=limit, Obj=OneData)

    def limitIdfromDate(self, date):

        datenum = date2astro(date)
        [[maxid, maxtai]] = self.sqlRequest('reply_raw', 'id,tai', order='order by tai desc', limit='limit 1')

        [[limId1]] = self.sqlRequest('reply_raw', 'id',
                                     where='where tai>%.2f and tai<%.2f' % (datenum, datenum + 86400), limit='limit 1')
        if (datenum + 86400) < maxtai:
            [[limId2]] = self.sqlRequest('reply_raw', 'id',
                                         where='where tai>%.2f and tai<%.2f' % (datenum + 86400, datenum + 2 * 86400),
                                         limit='limit 1')
        else:
            limId2 = maxid

        return limId1, limId2

    def closestId(self, table, limitId=False, date=False):
        if not limitId:
            limId1, limId2 = self.limitIdfromDate(date=date)
        else:
            limId1, limId2 = limitId

        [[closestId]] = self.sqlRequest(table, 'raw_id', where='where raw_id>%i and raw_id<%i' % (limId1, limId2),
                                        limit='limit 1')

        return closestId

    def allTables(self):
        cols = 'table_name'
        table = 'information_schema.tables'
        where = "where table_schema='public'"

        ignore = ['reply_raw', 'reply_hdr', 'actors', 'cmds', 'hub']
        array = self.sqlRequest(table=table, cols=cols, where=where)

        allTable = [table[0] for table in array if table[0].split('__')[0] not in ignore]

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

    def pollDbConf(self, date):
        allTables = self.allTables()
        fTables = DummyConf()
        for table in allTables:
            try:
                closestId = self.closestId(table=table, date=date)
                cols = self.allColumns(table)
                fTables.add(table, cols)

            except ValueError:
                pass

        return buildPfsConf(fTables)

    def loadMode(self):
        cols = 'id,tai,b1,r1,n1,b2,r2,n2,b3,r3,n3,b4,r4,n4,enu_sm1,enu_sm2,enu_sm3,enu_sm4,cleanroom,watercooling'
        modes = self.sqlRequest(table='mode', cols=cols, order='order by id desc', limit='limit 1')[0]

        return dict([(col, mode) for col, mode in zip(cols.split(','), modes)])

    def writeMode(self, device, mode):
        modes = self.loadMode()
        modes[device] = mode
        tai = date2num(dt.utcnow())
        id = int(modes['id']) + 1

        sqlRequest = """INSERT INTO mode VALUES (%i,%.4f,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
        id, tai, modes['b1'], modes['r1'], modes['n1'], modes['b2'], modes['r2'], modes['n2'], modes['b3'], modes['r3'],
        modes['n3'], modes['b4'], modes['r4'], modes['n4'], modes['enu_sm1'], modes['enu_sm2'], modes['enu_sm3'],
        modes['enu_sm4'], modes['cleanroom'], modes['watercooling'])
        conn = self.conn if self.conn else self.init()

        cursor = conn.cursor()
        cursor.execute(sqlRequest)
        conn.commit()

    def loadAlarms(self):
        modes = self.loadMode()
        modes.pop('id', None)
        modes.pop('tai', None)

        return loadAlarms(modes)



    def close(self):
        try:
            self.conn.close()
        except:
            pass
        self.conn = False
