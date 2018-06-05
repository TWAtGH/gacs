
from itertools import count


def setup_utils():
    global idgen
    idgen = count(1)


def next_id():
    return next(idgen)


def sizefmt(num, suffix='B', faktor=1024.0):
    assert faktor != 0
    units = ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']
    if faktor != 1024:
        units = ['','K','M','G','T','P','E','Z']
    for unit in units:
        if abs(num) < faktor:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= faktor
    return "%.1f%s%s" % (num, 'Yi', suffix)
