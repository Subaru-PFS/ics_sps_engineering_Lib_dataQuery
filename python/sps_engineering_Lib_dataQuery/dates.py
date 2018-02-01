import datetime as dt
from datetime import datetime

from matplotlib.dates import date2num, num2date


def str2date(datestr, fmt="%Y-%m-%dT%H:%M"):
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


def date2astro(datetime):
    return num2astro(date2num(datetime))
