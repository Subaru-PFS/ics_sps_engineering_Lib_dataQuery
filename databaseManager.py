import datetime as dt
import csv

import pytz
import psycopg2
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

    def initDatabase(self):
        try:
            property = "dbname='archiver' user='pfs' host='%s' port='%s'" % (self.ip, self.port)
            conn = psycopg2.connect(property)
            self.database = conn.cursor()
            self.conn = conn
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

    def getrowrelative2Date(self, tableName, keyword, date_num, force=False):
        if date_num is not -1:
            request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE tai > %f order by tai asc limit 1""" % (
                keyword, tableName, tableName, date_num)
        else:
            request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1 """ % (
                keyword, tableName, tableName)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
            self.reconnectDatabase()
            return -5
        try:
            [(row_result,)] = self.database.fetchall()
            return row_result
        except ValueError:
            if not force:
                return -4
            else:
                return self.getrowrelative2Date(tableName, keyword, -1)

    def getData(self, tableName, keyword, id_inf=0, id_sup="Now", convert=True):
        request = """select id, tai, %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i) order by id asc""" % (
            keyword, tableName, tableName, tableName,
            id_inf) if id_sup == "Now" else  """select id, tai, %s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i and %s.raw_id<=%i) order by id asc""" % (
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
            data_id = np.zeros(len(all_data))
            data_date = np.zeros(len(all_data))
            data_values = np.zeros((len(all_data), len(all_data[0]) - 2))
            for i, data in enumerate(all_data):
                data_id[i] = int(data[0])
                data_date[i] = self.convertfromAstro(data[1]) if convert else data[1]
                data_values[i] = data[2:]
            if convert:
                return data_id[-1], data_date, data_values
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
            return self.getData(tableName, keyword, beginning, end)
        else:
            return 0, beginning, end

    def extract2csv(self, tableName, keyword, label, beginning=0, end="Now", calibrate=False, path=""):

        id, dates, values = self.getDataBetween(tableName, keyword, beginning, end)
        if type(dates) == np.ndarray:
            if dates.any():
                first_row = ["Time Stamp"]
                first_row.extend(label.split(','))

                if not calibrate:
                    path += '%s.csv' % tableName
                else:
                    path += "%s_recorrected.csv" % tableName

                with open(path, 'wb') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',',
                                            quotechar='|', quoting=csv.QUOTE_MINIMAL)

                    spamwriter.writerow(first_row)
                    for i, (date, value) in enumerate(zip(dates, values)):
                        row = [num2date(date).strftime("%d/%m/%Y %H:%M:%S")]
                        if calibrate:
                            value = self.recalibrateTemps(value)

                        format = "%.2f" if "pressure" not in tableName else "%.3e"
                        row.extend([format % val for val in value])
                        spamwriter.writerow(row)

                return 1
            else:
                return None
        else:
            return None

    def getDatafromDate(self, tableName, keyword, date):
        id = self.getrowrelative2Date(tableName, "id", self.convertTimetoAstro(date))
        if id not in [-1, -2, -3, -4]:
            value = []
            request = """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id where raw_id = %s""" % (
                keyword, tableName, tableName, id)
            try:
                self.database.execute(request)
            except psycopg2.ProgrammingError:
                self.conn.rollback()
                return -2, -2
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
        else:
            return id, id

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
