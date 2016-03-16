from time import strptime, mktime
import datetime
import csv

import psycopg2
from matplotlib.dates import date2num, num2date
import numpy as np
from scipy.optimize import fsolve
from astrotime import AstroTime


class databaseManager():
    def __init__(self, ip, port):

        self.error_code = {-1: "network_database", -2: "bad_request", -3: "bad date format", -4: "no_data"}
        self.ip = ip
        self.port = port

    def initDatabase(self):
        try:
            property = "dbname='archiver' user='pfs' host='%s' port='%s'" % (self.ip, self.port)
            conn = psycopg2.connect(property)
            self.database = conn.cursor()
            self.conn = conn
            return 1
        except psycopg2.OperationalError:
            return -1

    def getData_Numpy(self, deviceName, keyword, id_inf=0, id_sup=None, convert=True):
        request = """select tai, %s, id from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i) order by id asc""" % (
            keyword, deviceName, deviceName, deviceName,
            id_inf) if id_sup == None else  """select tai, %s, id from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i and %s.raw_id<=%i) order by id asc""" % (
            keyword, deviceName, deviceName, deviceName, id_inf, deviceName, id_sup)
        try:
            self.database.execute(request)

        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return [-2, -2], id_inf
        data = self.database.fetchall()
        if data:
            data_values = np.zeros(len(data))
            data_id = np.zeros(len(data))
            data_date = np.zeros(len(data))
            for i in range(len(data)):
                if convert:
                    data_date[i] = self.convertfromAstro(data[i][0])
                else:
                    data_date[i] = float(data[i][0])
                data_values[i] = float(data[i][1])
                data_id[i] = int(data[i][2])
            if convert:
                return [data_date, data_values], data_id[-1]
            else:
                return [data_date, data_values], data_id

        else:
            return [-4, -4], id_inf

    def getLastData(self, deviceName, keyword):
        value = []
        request = """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1""" % (
            keyword, deviceName, deviceName)
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

    def getLastDate(self, deviceName):
        request = """select tai from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1""" % (
            deviceName, deviceName)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        data = self.database.fetchall()
        if data:
            date = self.convertfromAstro(data[0][0])
            if date != 0:
                date = num2date(date).strftime("%d/%m/%Y %H:%M:%S")
                return date
            else:
                return -3
        else:
            return -4

    def getrowrelative2Date(self, deviceName, keyword, date_num, force=False):
        if date_num != -1:
            request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id WHERE tai > %f order by tai asc limit 1""" % (
                keyword, deviceName, deviceName, date_num)
        else:
            request = """select %s from reply_raw inner join %s on %s.raw_id=reply_raw.id order by raw_id desc limit 1 """ % (
                keyword, deviceName, deviceName)
        try:
            self.database.execute(request)
        except psycopg2.ProgrammingError:
            self.conn.rollback()
            return -2
        try:
            [(row_result,)] = self.database.fetchall()
            return row_result
        except ValueError:
            if not force:
                return -4
            else:
                return self.getrowrelative2Date(deviceName, keyword, -1)

    def getDatafromDate(self, deviceName, keyword, date):
        id = self.getrowrelative2Date(deviceName, "id", self.convertTimetoAstro(date))
        if id not in [-1, -2, -3, -4]:
            value = []
            request = """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id where raw_id = %s""" % (
                keyword, deviceName, deviceName, id)
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
        date_num = datetime.datetime.fromtimestamp(mktime(strptime(date, "%d/%m/%Y %H:%M:%S")))
        date_num = (date_num - datetime.datetime(1970, 1, 1)).total_seconds()
        date_num = AstroTime.fromtimestamp(date_num)
        date_num = date_num.MJD() * 86400
        return date_num

    def convertfromAstro(self, date):
        date = float(date)
        date = AstroTime.fromMJD(date / 86400)
        date = str(date).split("MJD")[0][:-1]
        try:
            datenum = date2num(datetime.datetime.fromtimestamp(mktime(strptime(date, "%Y-%m-%d %H:%M:%S.%f"))))
            return datenum
        except ValueError:
            try:
                datenum = date2num(datetime.datetime.fromtimestamp(mktime(strptime(date, "%Y-%m-%d %H:%M:%S"))))
                return datenum
            except ValueError:
                return 0

    def getDataBetween(self, column, keyword, beginning=None, end="Now"):

        if beginning != None:
            beginning = self.convertTimetoAstro(beginning)
            beginning = self.getrowrelative2Date(column, "id", beginning, force=True)
        if end != "Now":
            end = self.convertTimetoAstro(end)
        else:
            end = self.convertTimetoAstro(self.getLastDate(column))
        end = self.getrowrelative2Date(column, "id", end, force=True)

        if beginning not in [-4, -3, -2, -1] and end not in [-4, -3, -2, -1]:
            return self.getData(column, keyword, beginning, end)
        else:
            return None

    def getData(self, column, keyword, id_inf, id_sup):

        if id_inf < id_sup:
            self.database.execute(
                """select tai,%s from reply_raw inner join %s on %s.raw_id=reply_raw.id where (%s.raw_id>%i and %s.raw_id<=%i) order by tai asc""" % (
                    keyword,
                    column, column, column, id_inf, column, id_sup))
            data = self.database.fetchall()
            result = np.zeros((len(data), len(data[0])))
            for i, (res, dat) in enumerate(zip(result, data)):
                res[0] = self.convertfromAstro(dat[0])
                res[1:] = dat[1:]
            return result

    def extract2csv(self, column, keyword, label, beginning=None, end="Now", conversion=False, path=""):

        data = self.getDataBetween(column, keyword, beginning, end)
        if data is not None:
            first_row = ["Time Stamp"]
            first_row.extend(label.split(','))

            if not conversion:
                path += '%s.csv' % column
            else:
                path += "%s_recorrected.csv" % column

            with open(path, 'wb') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)

                spamwriter.writerow(first_row)

                for i, line in enumerate(data):
                    row = [num2date(line[0]).strftime("%d/%m/%Y %H:%M:%S")]
                    if conversion:
                        line[1:] = self.recalibrateTemps(line[1:])
                    row.extend(["%0.2f" % val for val in line[1:]])
                    spamwriter.writerow(row)

            return 1
        else:
            return None

    def recalibrateTemps(self, data):
        # craigCoeffs = np.array([[0.98555011, -0.04371074],
        # [0.98076066, -0.04474339],
        # [0.99328676, -0.04429770],
        # [0.99484957, -0.04424603],
        # [1.01602077, -0.04135464],
        # [1.00978633, -0.04253082],
        # [1.01484596, -0.04503893]])

        # joeCoeffs = np.array(
        # [0.9851980849, 0.9810931459, 0.9938543011, 0.9970195074, 1.0190557483, 1.0125327739, 1.0166231335])

        # for val, coeff in zip(data, craigCoeffs):
        # res.append((val + coeff[1] * 273.15) / (coeff[0] + coeff[1]))
        # res = []
        # for val in data:
        #     res.append((val-(-0.000116592*val**2-0.034885583*val+16.92037997)))
        # return res

        res = []
        for val in data:
            res.append(val)

        for val in data:
            popt = (val, 1.503395E+01, 3.422648E-01, -3.990938E-04, 7.077527E-07, -5.484495E-10, 1.558666E-13)
            res.append(self.rational_fit(fsolve(self.inv_poly, 1000, args=popt) / 10))
        return res

    def inv_poly(self, r, *coeff):
        t, c0, c1, c2, c3, c4, c5 = coeff
        return t - (c0 + c1 * r + c2 * r ** 2 + c3 * r ** 3 + c4 * r ** 4 + c5 * r ** 5)

    def rational_fit(self, R, c0=-2.66035279e+02, c1=1.05064238e+01, c2=5.33902138e-01, c3=1.33904997e-02,
                     c4=-7.57068198e-05,
                     c5=2.33862917e-01, c6=5.08215684e-03, c7=-2.92984427e-05):
        return c0 + 273.15 + R * (c1 + R * (c2 + R * (c3 + c4 * R))) / (1 + R * (c5 + R * (c6 + c7 * R)))
