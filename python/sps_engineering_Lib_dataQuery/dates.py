import datetime as dt
from datetime import datetime

from matplotlib.dates import date2num, num2date


def str2date(datestr):

    if len (datestr)==10:
        fmt="%Y-%m-%d"
    elif len (datestr)==16:
        fmt="%Y-%m-%d"+datestr[10]+"%H:%M"
    elif len (datestr)==19:
        fmt="%Y-%m-%d"+datestr[10]+"%H:%M:%S"
    else:
        fmt="%Y-%m-%d"+datestr[10]+"%H:%M:%S.%f"

    return dt.datetime.strptime(datestr, fmt)


def str2astro(datestr):
    return num2astro(date2num(str2date(datestr)))


def num2astro(datenum):
    offset = 50000 - date2num(dt.datetime(1995, 10, 10))
    return (datenum + offset) * 86400


def astro2num(astrotime):
    return (astrotime / 86400) - 50000 + date2num(datetime(1995, 10, 10))


def astro2date(astrotime):
    return num2date(astro2num(astrotime))


def date2astro(date):
    datetime = str2date(date) if isinstance(date, str) else date
    return num2astro(date2num(datetime))


def all2num(date):

    if isinstance(date, float):
        return date
    if isinstance(date, int):
        return date
    elif isinstance(date, str):
        datetime = str2date(date)
        return date2num(datetime)
    elif isinstance(date, dt.datetime):
        return date2num(date)
    elif isinstance(date, unicode):
        return all2num(str(date))
    else:
        raise ValueError

