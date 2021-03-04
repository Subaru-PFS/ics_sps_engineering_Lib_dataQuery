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
    def __init__(self, *args, **kwargs):
        pd.DataFrame.__init__(self, *args, **kwargs)
        self.fillna(value=np.nan, inplace=True)
        self['id'] = self['id'].astype('int64')

    @property
    def strdate(self):
        return num2date(self['tai']).isoformat()[:19]


class OneData(PfsData):
    def __init__(self, *args, **kwargs):
        PfsData.__init__(self, *args, **kwargs)

    def __getitem__(self, key):
        vals = pd.DataFrame.__getitem__(self, key)
        return vals[0] if len(vals) == 1 else vals


class DatabaseManager(object):
    def __init__(self, host='db-ics', port=5432, password=None, dbname='archiver', user='pfs', doConnect=True):
        self.prop = dict(host=host,
                         port=port,
                         password=password,
                         dbname=dbname,
                         user=user)

        self.alarmPath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'alarm'))
        self.configPath = os.path.abspath(os.path.join(os.path.dirname(dataQuery.__file__), '../..', 'config'))

        if doConnect:
            self.connect()

    @property
    def activeConn(self):
        conn = self.conn if self.conn else self.connect()
        return conn

    def connect(self):
        self.nq = 0
        self.conn = psycopg2.connect(**self.prop)
        return self.conn

    def fetchall(self, query, doRetry=True):
        if self.nq > 1000:
            self.close()

        with self.activeConn.cursor() as curs:
            try:
                curs.execute(query)
                self.nq += 1

            except Exception as e:
                self.activeConn.rollback()
                if doRetry:
                    return self.fetchall(query, doRetry=False)
                raise

            return np.array(curs.fetchall())

    def fetchone(self, query, doRetry=True):
        if self.nq > 1000:
            self.close()

        with self.activeConn.cursor() as curs:
            try:
                curs.execute(query)
                self.nq += 1

            except Exception as e:
                self.activeConn.rollback()
                if doRetry:
                    return self.fetchone(query, doRetry=False)
                raise

            return curs.fetchone()

    def dataBetweenId(self, table, cols, minId, maxId=False, convert=True):
        minId = int(minId)
        maxId, maxTai = self.fetchone(self.rangeMax(end=False)) if not maxId else (int(maxId), -1)

        dataQuery = f'select id,tai,{cols} from (select * from {table} where raw_id>={minId} and raw_id<={maxId} ) ' \
                    f'as data join reply_raw as reply on data.raw_id=reply.id order by id asc'

        rawData = self.fetchall(dataQuery)

        if not rawData.size:
            raise ValueError('no raw data : %s' % dataQuery)

        if convert:
            rawData[:, 1] = astro2num(rawData[:, 1])

        return PfsData(rawData, columns=['id', 'tai'] + cols.split(','))

    def rangeMax(self, end=False):
        query = 'select id,tai from reply_raw order by id desc limit 1'
        maxId, maxTai = self.fetchone(query)
        if end:
            endTai = str2astro(end)
            if endTai < maxTai:
                query = f'select id,tai from reply_raw where tai>={endTai} order by tai asc limit 1'

        return query

    def dataBetween(self, table, cols, start, end=False, convert=True):
        rngminQuery = f'(select id from reply_raw where tai>={str2astro(start)} order by tai asc limit 1) as rngmin'
        rngmaxQuery = f'({self.rangeMax(end=end)}) as rngmax'

        minId, maxId = self.fetchone("""select rngmin.id, rngmax.id from %s, %s""" % (rngminQuery, rngmaxQuery))
        return self.dataBetweenId(table, cols, minId, maxId=maxId, convert=convert)

    def last(self, table, cols=''):
        lastRow = self.fetchall('select raw_id,%s from %s order by raw_id desc limit 1' % (cols, table))
        lastRow = OneData(lastRow, columns=['id'] + cols.split(','))
        try:
            tai, = self.fetchone('select tai from reply_raw where id=%d' % lastRow.id)
            lastRow['tai'] = astro2num(tai)
            return lastRow
        except ValueError:
            time.sleep(0.02)
            return self.last(table=table, cols=cols)

    def limitIdfromDate(self, date, reverse=False):

        datenum = date2astro(date)
        mintai, maxtai = (datenum, datenum + 86400) if not reverse else (datenum - 86400, datenum)

        minId, = self.fetchone('select id from reply_raw where tai>%.2f and '
                               'tai<%.2f order by tai asc limit 1' % (mintai, maxtai))
        maxId, = self.fetchone('select id from reply_raw where tai>%.2f and '
                               'tai<%.2f order by tai desc limit 1' % (mintai, maxtai))

        return minId, maxId

    def idFromDate(self, table, date, reverse=False):
        minId, maxId = self.limitIdfromDate(date=date, reverse=reverse)
        return self.closestId(table, minId, maxId, reverse)

    def closestId(self, table, minId, maxId, reverse=False):
        order = 'asc' if not reverse else 'desc'
        closestId, = self.fetchone('select raw_id from %s where raw_id>=%i '
                                   'and raw_id<=%i order by raw_id %s limit 1' % (table, minId, maxId, order))
        return closestId

    def allTables(self):
        ignore = ['reply_raw', 'reply_hdr', 'actors', 'cmds', 'hub']
        array = self.fetchall("select table_name from information_schema.tables where table_schema='public'")

        allTable = [table[0] for table in array if table[0].split('__')[0] not in ignore]

        return allTable

    def allColumns(self, tablename):
        where = "(table_schema='public' AND table_name='%s' and data_type !='text' and column_name!='raw_id')" % tablename
        array = self.fetchall("select column_name from information_schema.columns where %s" % where)

        allCols = [col[0] for col in array]
        if allCols:
            return allCols
        else:
            raise ValueError('No columns')

    def pollDbConf(self, date):
        allTables = self.allTables()
        fTables = DummyConf()

        try:
            minId, maxId = self.limitIdfromDate(date=date)
        except (ValueError, TypeError):
            return buildPfsConf(fTables)

        for table in allTables:
            try:
                closestId = self.closestId(table, minId=minId, maxId=maxId)
                cols = self.allColumns(table)
                fTables.add(table, cols)

            except:
                pass

        return buildPfsConf(fTables)

    def close(self):
        try:
            self.conn.close()
        except:
            pass
        self.conn = False
