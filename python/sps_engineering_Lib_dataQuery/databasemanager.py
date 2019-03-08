import os
import time
import numpy as np
import pandas as pd
import psycopg2
import sps_engineering_Lib_dataQuery as dataQuery
from matplotlib.dates import num2date
from sps_engineering_Lib_dataQuery.confighandler import DummyConf, buildPfsConf
from sps_engineering_Lib_dataQuery.dates import astro2num, str2astro, date2astro


class PfsData(pd.DataFrame):
    def __init__(self, data):
        pd.DataFrame.__init__(self, data=data)

    @property
    def strdate(self):
        return num2date(self['tai']).isoformat()[:19]


class OneData(PfsData):
    def __init__(self, data):
        PfsData.__init__(self, data=data)

    def __getitem__(self, key):
        vals = pd.DataFrame.__getitem__(self, key)
        return vals[0] if len(vals) == 1 else vals


class DatabaseManager(object):
    def __init__(self, ip, port, password, dbname='archiver'):

        self.ip = ip
        self.port = port
        self.dbname = dbname
        self.password = password

        self.conn = False
        self.alarmPath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'alarm'))
        self.configPath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'config'))

    def init(self):
        self.nq = 0
        prop = "dbname='%s' user='pfs' password='%s' host='%s' port='%s'" % (self.dbname, self.password, self.ip,
                                                                             self.port)
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

    def pfsdata(self, table, cols='', where='', order='', limit='', convert=True, Obj=PfsData):
        joinTable = 'reply_raw inner join %s on %s.raw_id=reply_raw.id' % (table, table)
        typedCols = [('id', '<i8'), ('tai', '<f8')] + [(str(col), '<f8') for col in cols.split(',') if col]
        cols = ','.join([name for name, type in typedCols])

        rawData = self.sqlRequest(table=joinTable,
                                  cols=cols,
                                  where=where,
                                  order=order,
                                  limit=limit)
        if not rawData.size:
            raise ValueError('no raw data : select %s from %s %s %s %s ' % (cols, joinTable, where, order, limit))

        if convert:
            rawData[:, 1] = astro2num(rawData[:, 1])

        data = np.array([tuple(row) for row in list(rawData)], dtype=typedCols)

        return Obj(data=data)

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

    def last(self, table, cols=''):
        [[lastId]] = self.sqlRequest(cols='max(raw_id)', table=table)
        try:
            return self.pfsdata(table, cols=cols, where='where id=%d' % lastId, Obj=OneData)
        except ValueError:
            time.sleep(0.02)
            return self.last(table=table, cols=cols)

    def limitIdfromDate(self, date, reverse=False):

        datenum = date2astro(date)
        mintai, maxtai = (datenum, datenum + 86400) if not reverse else (datenum - 86400, datenum)

        [[minid]] = self.sqlRequest('reply_raw', 'id',
                                    where='where tai>%.2f and tai<%.2f' % (mintai, maxtai),
                                    limit='limit 1')

        [[maxid]] = self.sqlRequest('reply_raw', 'id',
                                    where='where tai>%.2f and tai<%.2f' % (mintai, maxtai),
                                    order='order by tai desc',
                                    limit='limit 1')
        return minid, maxid

    def closestId(self, table, limitIds=None, date=False, reverse=False):
        if not date:
            minid, maxid = limitIds
        else:
            minid, maxid = self.limitIdfromDate(date=date, reverse=reverse)

        order = 'order by raw_id asc' if not reverse else 'order by raw_id desc'
        [[closestId]] = self.sqlRequest(table, 'raw_id', where='where raw_id>%i and raw_id<%i' % (minid, maxid),
                                        order=order,
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
        limitIds = self.limitIdfromDate(date=date)
        for table in allTables:
            try:
                closestId = self.closestId(table=table, limitIds=limitIds)
                cols = self.allColumns(table)
                fTables.add(table, cols)

            except ValueError:
                pass

        return buildPfsConf(fTables)

    def close(self):
        try:
            self.conn.close()
        except:
            pass
        self.conn = False
