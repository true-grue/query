# See Rompf, Tiark, and Nada Amin. "A SQL to C compiler in 500 lines of code." (2019).

from pathlib import Path
from collections import namedtuple

Row = namedtuple('Row', 'fields schema')


def Scan(filename):
    lines = Path(filename).read_text().strip().split('\n')
    schema = lines.pop(0).split(',')
    for line in lines:
        yield Row(line.split(','), schema)


def select_fields(row, keys):
    return [row.fields[row.schema.index(key)] for key in keys]


def Print(parent):
    for row in parent:
        print(*row.fields)


def Filter(pred, parent):
    for row in parent:
        if pred(row):
            yield row


def Eq(x, y):
    def func(row):
        return x(row) == y(row)
    return func


def Ne(x, y):
    def func(row):
        return x(row) != y(row)
    return func


def Value(x):
    def func(row):
        return x
    return func


def Field(x):
    def func(row):
        return select_fields(row, [x])[0]
    return func


def Project(new_schema, parent_schema, parent):
    for row in parent:
        yield Row(select_fields(row, parent_schema), new_schema)


def Join(left, right):
    right = list(right)
    for row1 in left:
        for row2 in right:
            keys = list(set(row1.schema) & set(row2.schema))
            if select_fields(row1, keys) == select_fields(row2, keys):
                yield Row(row1.fields + row2.fields, row1.schema + row2.schema)


Print(Project(["room", "title"], ["room", "title"],
              Filter(Eq(Field("time"), Value("09:00 AM")), Scan("talks.csv"))))


Print(Filter(Ne(Field("title1"), Field("title2")), Join(
    Project(["time", "room", "title1"], [
            "time", "room", "title"], Scan("talks.csv")),
    Project(["time", "room", "title2"], ["time", "room", "title"], Scan("talks.csv")))
))
