import datetime as dt
import csv

import pytz
import psycopg2
from datetime import datetime, timedelta
from matplotlib.dates import date2num, num2date
import numpy as np

from astrotime import AstroTime


class databaseManager():
    def __init__(self, ip, port):

        self.error_code = {-1: "network_database", -2: "bad_request", -3: "bad date format", -4: "no_data",
                           -5: "network disconnected"}
        self.ip = ip
        self.port = port
        self.reconnecting = False

    def initDatabase(self, dbname='archiver'):
        self.dbname = dbname
        try:
            property = "dbname='%s' user='pfs' host='%s' port='%s'" % (dbname, self.ip, self.port)
            conn = psycopg2.connect(property)
            self.database = conn.cursor()
            self.conn = conn
            self.database.execute("""select tai from reply_raw order by id asc limit 1""")
            return 1
        except psycopg2.OperationalError:
            return -1

    def reconnectDatabase(self):
        if not self.reconnecting:
            self.reconnecting = True
            err = self.initDatabase()
            self.reconnecting = False
        else:
            pass

    def closeDatabase(self):
        self.database.close()
        self.conn.close()

    def getrowrelative2Date(self, tableName, keyword, date_num, force=False):
        try:
            self.database.execute("""select raw_id from %s order by raw_id asc limit 1""" % tableName)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        [(id_inf,)] = self.database.fetchall()
        self.database.execute("""select raw_id from %s order by raw_id desc limit 1""" % tableName)
        [(id_sup,)] = self.database.fetchall()
        self.database.execute("""select id from reply_raw WHERE (tai >= %f and tai < %f) order by id asc limit 1 """ % (
            date_num, date_num + 60))
        try:
            [(close_id,)] = self.database.fetchall()
        except ValueError:
            return -4
        if force:
            if close_id < id_inf:
                request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where id=%i""" % (
                    keyword, tableName, tableName, id_inf)
            elif close_id > id_sup:
                request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where id=%i""" % (
                    keyword, tableName, tableName, id_sup)
            else:
                request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE tai >= %f order by id asc limit 1""" % (
                    keyword, tableName, tableName, date_num)
        else:
            if id_inf < close_id <= id_sup:
                request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE (tai >= %f and tai < %f) order by id asc limit 1""" % (
                    keyword, tableName, tableName, date_num, date_num + 120)
            else:
                return -4
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
            self.reconnectDatabase()
            return -5
        try:
            [res] = self.database.fetchall()
            if len(res) == 1:
                return res[0]
            else:
                return list(res)
        except ValueError:
            return -4

    def getData(self, tableName, keyword, id_inf=0, id_sup="Now", convert=True):
        request = """select id, tai, %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i) order by id asc""" % (
            keyword, tableName, tableName, tableName,id_inf) if id_sup == "Now" else  """select id, tai, %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i and %s.raw_id<=%i) order by id asc""" % (
            keyword, tableName, tableName, tableName, id_inf, tableName, id_sup)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return id_inf, -2, -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
            self.reconnectDatabase()
            return id_inf, -5, -5
        all_data = self.database.fetchall()
        if all_data:
            data_id = np.array([data[0] for data in all_data], dtype=np.int64)
            data_date = np.array([data[1] for data in all_data], dtype=np.float64)
            data_values = np.array([data[2:] for data in all_data], dtype=np.float64)
            if convert:
                data_date = data_date/86400-50000
                data_date = np.array([datetime(1995,10,10) + timedelta(days=a) for a in data_date])
                return data_id[-1], date2num(data_date), data_values
            else:
                return data_id, data_date, data_values

        else:
            return id_inf, -4, -4

    def getLastData(self, tableName, keyword):
        value = []
        request = """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1""" % (
            keyword, tableName, tableName)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2, -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
            self.reconnectDatabase()
            return -5, -5
        data = self.database.fetchall()
        if data:
            date = self.convertfromAstro(data[0][0])
            if date != 0:
                date = num2date(date).strftime("%d/%m/%Y %H:%M:%S")
                for val in data[0][1:]:
                    value.append(val)
                return date, value
            else:
                return -3, -3
        else:
            return -4, -4

    def getDataBetween(self, tableName, keyword, beginning=0, end="Now"):

        if beginning != 0:
            beginning = self.convertTimetoAstro(beginning)
            beginning = self.getrowrelative2Date(tableName, "id", beginning, force=True)
        if end != "Now":
            end = self.convertTimetoAstro(end)
            end = self.getrowrelative2Date(tableName, "id", end, force=True)

        if beginning not in [-4, -3, -2, -1] and end not in [-4, -3, -2, -1]:
            id, data_date, data_values = self.getData(tableName, keyword, beginning, end)
            return data_date, data_values
        else:
            return 0, beginning, end

    def extract2csv(self, tableName, keyword, label, beginning=0, end="Now", path=""):

        id, dates, values = self.getDataBetween(tableName, keyword, beginning, end)
        if type(dates) == np.ndarray:
            if dates.any():
                first_row = ["Time Stamp"]
                first_row.extend(label.split(','))
                path += '%s.csv' % tableName
                with open(path, 'wb') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',',
                                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    spamwriter.writerow(first_row)
                    for i, (date, value) in enumerate(zip(dates, values)):
                        row = [num2date(date).strftime("%d/%m/%Y %H:%M:%S")]

                        format = "%.2f" if "pressure" not in tableName else "%.3e"
                        row.extend([format % val for val in value])
                        spamwriter.writerow(row)

                return 1
            else:
                return None
        else:
            return None

    def getDatafromDate(self, tableName, keyword, date):
        res = self.getrowrelative2Date(tableName, 'tai,' + keyword, self.convertTimetoAstro(date))
        if res not in [-5, -4, -3, -2, -1]:
            return num2date(self.convertfromAstro(res[0])).strftime("%d/%m/%Y %H:%M:%S"), res[1:]
        else:
            return np.nan, np.nan

    def convertTimetoAstro(self, date):
        date_num = dt.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
        date_num = (date_num - dt.datetime(1970, 1, 1)).total_seconds()
        date_num = AstroTime.fromtimestamp(date_num, tz=pytz.utc)
        date_num = date_num.MJD() * 86400
        return date_num

    def convertfromAstro(self, date):
        date = float(date)
        date = AstroTime.fromMJD(date / 86400)
        date = str(date).split("MJD")[0][:-1]
        try:
            datenum = date2num(dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f"))
            return datenum
        except ValueError:
            try:
                datenum = date2num(dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))
                return datenum
            except ValueError:
                return 0
