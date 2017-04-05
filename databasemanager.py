import csv
import datetime as dt
from datetime import datetime, timedelta

import numpy as np
import psycopg2
from matplotlib.dates import date2num, num2date


class DatabaseManager():
    def __init__(self, ip, port):

        self.error_code = {-1: "network_database", -2: "bad_request", -3: "bad date format", -4: "no_data",
                           -5: "network disconnected, attempting connection"}
        self.ip = ip
        self.port = port
        self.database = None
        self.conn = None
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

    def getrowrelative2Date(self, tableName, keyword, date_num, force=False, reverse=False, i=0):

        nb_sec = 600

        if i == 0:
            test = self.getLastData(tableName, "raw_id")
            if type(test) == int:
                return test
            else:
                last_date, last_datas = test
                last_date = self.convertTimetoAstro(last_date)
                if last_date < date_num:
                    return -4

        if not reverse:
            request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE (tai >= %f and tai < %f) order by id asc limit 1""" % (
                keyword, tableName, tableName, date_num, date_num + nb_sec)
            date_num += nb_sec
        else:
            request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE (tai >= %f and tai < %f) order by id desc limit 1""" % (
                keyword, tableName, tableName, date_num - nb_sec, date_num)
            date_num -= nb_sec

        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError, AttributeError):
            self.reconnectDatabase()
            return -5
        try:
            [res] = self.database.fetchall()
            if len(res) == 1:
                return res[0]
            else:
                return np.asarray(res)
        except ValueError:
            if force and i < int(((23 * 3600) / nb_sec)):
                return self.getrowrelative2Date(tableName, keyword, date_num, force, reverse, i + 1)
            else:
                return -4
        except psycopg2.ProgrammingError:
            return -4

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
        except (psycopg2.InterfaceError, psycopg2.DatabaseError, AttributeError):
            self.reconnectDatabase()
            return -5
        all_data = self.database.fetchall()
        array = np.asarray(all_data)
        if array.size:
            try:
                data_id = np.asarray(array[:, 0], dtype=np.int64)
                data_date = np.asarray(array[:, 1], dtype=np.float64)
                data_values = np.asarray(array[:, 2:], dtype=np.float64)
            except IndexError:
                return -4

            if convert:
                data_date = self.convertArraytoAstro(data_date)
                return data_id[-1], data_date, data_values
            else:
                return data_id, data_date, data_values

        else:
            return -4

    def getLastData(self, tableName, keywords):
        keyOne = keywords.split(',')[0]

        request = """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id where %s is not null order by raw_id desc limit 1""" % (
            keywords, tableName, tableName, keyOne)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            print request
            self.conn.rollback()
            return -2
        except (psycopg2.InterfaceError, psycopg2.DatabaseError, AttributeError):
            self.reconnectDatabase()
            return -5
        try:
            data = self.database.fetchall()
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        if type(data) == list:
            if len(data) == 1:
                if len(data[0]) > 1:
                    date = (datetime(1995, 10, 10) + timedelta(days=((data[0][0] / 86400) - 50000))).strftime(
                        "%d/%m/%Y %H:%M:%S")
                    values = np.asarray(data[0][1:])
                    return date, values
        return -4

    def getDataBetween(self, tableName, keyword, beginning=0, end=np.inf):

        if beginning != 0:
            beginning = self.convertTimetoAstro(beginning)
            beginning = self.getrowrelative2Date(tableName, "id", beginning, force=True)
            if beginning < 0:
                return beginning
        if end != np.inf:
            end = self.convertTimetoAstro(end)
            end = self.getrowrelative2Date(tableName, "id", end, force=True, reverse=True)
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
        offset = 50000 - date2num(dt.datetime(1995, 10, 10))
        return (date2num(dt.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")) + offset) * 86400

    def convertfromAstro(self, date):
        return date / 86400 - 50000 + date2num(datetime(1995, 10, 10))

    def convertArraytoAstro(self, dates):
        offset = date2num(datetime(1995, 10, 10)) * np.ones(len(dates))
        res = dates / 86400 - 50000 + offset
        return res

    def getStatus(self, error_code):
        return self.error_code[error_code]
