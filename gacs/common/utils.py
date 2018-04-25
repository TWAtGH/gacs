
from itertools import count


def setup_utils():
    global idgen
    idgen = count(1)


def next_id():
    return next(idgen)
