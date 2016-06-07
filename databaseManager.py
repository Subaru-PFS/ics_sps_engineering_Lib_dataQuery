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
            return True
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

    def getrowrelative2Date(self, tableName, keyword, date_num, force=False, i=0):

        nb_sec = 3600
        request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE (tai >= %f and tai < %f) order by id asc limit 1""" % (
            keyword, tableName, tableName, date_num, date_num + nb_sec)
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
                return np.asarray(res)
        except ValueError:
            if force and i == 0:
                if self.tableIsInRange(tableName, date_num):
                    return self.getrowrelative2Date(tableName, keyword, date_num+nb_sec, True, i + 1)
                else:
                    return -4
            elif force and i < 23:
                return self.getrowrelative2Date(tableName, keyword, date_num+nb_sec, True, i + 1)
            else:
                return -4

    def tableIsInRange(self, tableName, date_num):

        self.database.execute(
            """select tai from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id asc limit 1""" % (
            tableName, tableName))
        [(tai_inf,)] = self.database.fetchall()
        self.database.execute(
            """select tai from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1""" % (
            tableName, tableName))
        [(tai_sup,)] = self.database.fetchall()
        if tai_inf < date_num <= tai_sup:
            return True
        else:
            return False



    def getData(self, tableName, keyword, id_inf=0, id_sup=np.inf, convert=True):
        request = """select id, tai, %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i) order by id asc""" % (
            keyword, tableName, tableName, tableName,
            id_inf) if id_sup == np.inf else  """select id, tai, %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i and %s.raw_id<=%i) order by id asc""" % (
            keyword, tableName, tableName, tableName, id_inf, tableName, id_sup)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
            self.reconnectDatabase()
            return -5
        all_data = self.database.fetchall()
        if all_data:
            data_id = np.array([data[0] for data in all_data], dtype=np.int64)
            data_date = np.array([data[1] for data in all_data], dtype=np.float64)
            data_values = np.array([data[2:] for data in all_data], dtype=np.float64)
            if convert:
                data_date = self.convertArraytoAstro(data_date)
                return data_id[-1], data_date, data_values
            else:
                return data_id, data_date, data_values

        else:
            return -4

    def getLastData(self, tableName, keyword):
        request = """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1""" % (
            keyword, tableName, tableName)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
            self.reconnectDatabase()
            return -5
        data = self.database.fetchall()
        if data:
            date = (datetime(1995, 10, 10) + timedelta(days=((data[0][0] / 86400) - 50000))).strftime(
                "%d/%m/%Y %H:%M:%S")
            values = np.asarray(data[0][1:])
            return date, values
        else:
            return -4

    def getDataBetween(self, tableName, keyword, beginning=0, end=np.inf):

        if beginning != 0:
            beginning = self.convertTimetoAstro(beginning)
            beginning = self.getrowrelative2Date(tableName, "id", beginning, force=True)
            if beginning < 0:
                return beginning
        if end != np.inf:
            end = self.convertTimetoAstro(end)
            end = self.getrowrelative2Date(tableName, "id", end, force=True)
            if end < 0:
                return end

        return_values = self.getData(tableName, keyword, beginning, end)
        if type(return_values) is not int:
            id, data_date, data_values = return_values
            return data_date, data_values
        else:
            return return_values

    def extract2csv(self, tableName, keyword, label, units, beginning=0, end=np.inf, path=""):
        formats = ["{:.3e}" if uni.strip() in ['Torr', 'mBar', 'Bar'] else '{:.2f}' for uni in units.split(',')]
        return_values = self.getDataBetween(tableName, keyword, beginning, end)
        if type(return_values) is not int:
            dates, values = return_values
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
                        row.extend([fmt.format(val) for fmt, val in zip(formats, value)])
                        spamwriter.writerow(row)

                return 1
            else:
                return None
        else:
            return None

    def getDatafromDate(self, tableName, keyword, date):
        return_values = self.getrowrelative2Date(tableName, 'tai,' + keyword, self.convertTimetoAstro(date))
        if type(return_values) is not int:
            return num2date(self.convertfromAstro(return_values[0])).strftime("%d/%m/%Y %H:%M:%S"), return_values[1:]
        else:
            return return_values

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

    def convertArraytoAstro(self, dates):
        dates = dates / 86400 - 50000
        dates = np.array([datetime(1995, 10, 10) + timedelta(days=a) for a in dates])
        return date2num(dates)
