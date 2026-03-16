import os
import time

import numpy as np
import pandas as pd
import psycopg
from matplotlib.dates import num2date
from sps_engineering_Lib_dataQuery import confighandler as conf
from sps_engineering_Lib_dataQuery.dates import astro2num, str2astro, date2astro


class PfsData(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        """DataFrame subclass that normalises NaNs and casts id to int64."""
        super().__init__(*args, **kwargs)
        self.fillna(value=np.nan, inplace=True)
        self['id'] = self['id'].astype('int64')

    @property
    def strdate(self):
        """Return the tai timestamp of the first row as an ISO 8601 string."""
        return num2date(self['tai']).isoformat()[:19]


class OneData(PfsData):
    def __getitem__(self, key):
        """Return a scalar when the column has exactly one row, otherwise the full Series."""
        vals = super().__getitem__(key)
        return vals.iloc[0] if len(vals) == 1 else vals


class DatabaseManager:
    def __init__(self, host='db-ics', port=5432, password=None, dbname='archiver', user='pfs', doConnect=True):
        """Initialise connection parameters and optionally connect to the database."""
        self.conn = None
        self.prop = dict(host=host, port=port, password=password, dbname=dbname, user=user)
        self.alarmPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'alarm'))
        self.configPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'config'))
        self.nq = 0
        if doConnect:
            self.connect()

    def connect(self):
        """Open a new database connection and reset the query counter."""
        self.conn = psycopg.connect(**self.prop, autocommit=True)
        self.nq = 0
        return self.conn

    @property
    def activeConn(self):
        """Return the current connection, opening one if needed."""
        return self.conn or self.connect()

    def _execute(self, query, fetch='all', doRetry=True):
        """Execute a query, recycling the connection after 1000 queries and retrying once on error."""
        if self.nq > 1000:
            self.close()
        with self.activeConn.cursor() as curs:
            try:
                curs.execute(query)
                self.nq += 1
                return curs.fetchall() if fetch == 'all' else curs.fetchone()
            except Exception:
                if doRetry:
                    return self._execute(query, fetch, doRetry=False)
                raise

    def fetchall(self, query, doRetry=True):
        """Execute a query and return all rows as a numpy array."""
        return np.array(self._execute(query, 'all', doRetry))

    def fetchone(self, query, doRetry=True):
        """Execute a query and return a single row."""
        return self._execute(query, 'one', doRetry)

    def getStartId(self, start):
        """Return the first reply_raw id at or after the given start date."""
        return self.fetchone(
            f"select id from reply_raw where tai>={str2astro(start)} order by tai asc limit 1")[0]

    def getEndId(self, end=False):
        """Return the first reply_raw id at or after end date, or the latest id if end is not given."""
        if end:
            return self.getStartId(end)
        return self.fetchone('select id from reply_raw order by id desc limit 1')[0]

    def dataBetweenId(self, table, cols, startId, endId=None, convert=True):
        """Fetch rows from table whose raw_id falls between startId and endId."""
        startId = int(startId)
        endId = endId or self.getEndId()

        query = (f"select r.id, r.tai, {cols} from {table} d "
                 f"join reply_raw r on r.id = d.raw_id "
                 f"where d.raw_id between {startId} and {endId}")
        data = self.fetchall(query)

        if not data.size:
            raise ValueError(f'no raw data: {query}')

        if convert:
            data[:, 1] = astro2num(data[:, 1])

        return PfsData(data, columns=['id', 'tai'] + [c.strip() for c in cols.split(',')]).sort_values("tai")

    def dataBetween(self, table, cols, start, end=False, convert=True):
        """Fetch rows from table within the given date range."""
        return self.dataBetweenId(table, cols, self.getStartId(start), endId=self.getEndId(end), convert=convert)

    def last(self, table, cols=''):
        """Return the most recent row from table, retrying if the tai lookup races with an insert."""
        row = self.fetchall(f'select raw_id,{cols} from {table} order by raw_id desc limit 1')
        lastRow = OneData(row, columns=['id'] + [c.strip() for c in cols.split(',')])
        try:
            tai = self.fetchone(f'select tai from reply_raw where id={lastRow.id}')[0]
            lastRow['tai'] = astro2num(tai)
            return lastRow
        except ValueError:
            time.sleep(0.02)
            return self.last(table=table, cols=cols)

    def limitIdfromDate(self, date, reverse=False):
        """Return the (startId, endId) reply_raw id range covering the 24h window around date."""
        datenum = date2astro(date)
        low, high = (datenum, datenum + 86400) if not reverse else (datenum - 86400, datenum)
        # Two LIMIT 1 queries are faster than MIN/MAX: the tai index allows an immediate boundary lookup.
        startId = self.fetchone(
            f'select id from reply_raw where tai>{low:.2f} and tai<{high:.2f} order by tai asc limit 1')[0]
        endId = self.fetchone(
            f'select id from reply_raw where tai>{low:.2f} and tai<{high:.2f} order by tai desc limit 1')[0]
        return startId, endId

    def idFromDate(self, table, date, reverse=False):
        """Return the closest raw_id in table for the given date, optionally searching backwards."""
        startId, endId = self.limitIdfromDate(date, reverse)
        return self.closestId(table, startId, endId, reverse)

    def closestId(self, table, startId, endId, reverse=False):
        """Return the first or last raw_id in table within [startId, endId]."""
        order = 'asc' if not reverse else 'desc'
        return self.fetchone(
            f'select raw_id from {table} where raw_id between {startId} and {endId} order by raw_id {order} limit 1')[0]

    def allTables(self, ignoreActors=None):
        """Return sorted list of actor__keyword tables, excluding ignored actors."""
        hardIgnore = {'cmds', 'hub', 'alerts'}
        default = {'dcb', 'dcb2', 'pfilamps', 'sps', 'iic', 'ag', 'agcc', 'fps', 'mcs'}
        ignore = hardIgnore | (default if ignoreActors is None else set(ignoreActors))

        def isRelevant(table):
            parts = table.split('__', 1)
            return len(parts) == 2 and parts[0] not in ignore

        tables = self.fetchall("select table_name from information_schema.tables where table_schema='public'")
        return sorted(filter(isRelevant, (t[0] for t in tables)))

    def allColumns(self, tablename):
        """Return non-text, non-raw_id column names for the given table."""
        where = (f"table_schema='public' AND table_name='{tablename}' "
                 "AND data_type != 'text' AND column_name != 'raw_id'")
        return [col[0] for col in self.fetchall(f"select column_name from information_schema.columns where {where}")]

    def pollDbConf(self, date, ignoreActors=None):
        """Build a PFS config from tables that have data for the given date."""
        fTables = conf.DummyConf()
        try:
            startId, endId = self.limitIdfromDate(date)
        except (ValueError, TypeError):
            return conf.buildPfsConf(fTables)

        tables = self.allTables(ignoreActors)
        if not tables:
            return conf.buildPfsConf(fTables)

        # Batch fetch columns for all tables in one query instead of one per table.
        table_list = ', '.join(f"'{t}'" for t in tables)
        cols_rows = self.fetchall(
            f"SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE table_schema='public' AND table_name IN ({table_list}) "
            f"AND data_type != 'text' AND column_name != 'raw_id' "
            f"ORDER BY table_name, ordinal_position"
        )
        table_cols = {}
        for table_name, col_name in cols_rows:
            table_cols.setdefault(table_name, []).append(col_name)

        # Batch check which tables have data in the ID range using a single UNION ALL.
        # Parentheses around each SELECT are required by PostgreSQL when combining LIMIT with UNION ALL.
        union_parts = [
            f"(SELECT '{t}' AS tname FROM {t} WHERE raw_id BETWEEN {startId} AND {endId} LIMIT 1)"
            for t in tables if t in table_cols
        ]
        active_rows = self.fetchall(' UNION ALL '.join(union_parts))
        active_tables = {row[0] for row in active_rows}

        for table in tables:
            if table in active_tables and table_cols.get(table):
                fTables.add(table, table_cols[table])

        return conf.buildPfsConf(fTables)

    def getDataType(self, tableName, columnName):
        """Return the PostgreSQL data type of a column."""
        return self.fetchone(
            f"SELECT data_type FROM information_schema.columns "
            f"WHERE table_name='{tableName}' AND column_name='{columnName}'")[0]

    def close(self):
        """Close the database connection and reset it to None."""
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = None
