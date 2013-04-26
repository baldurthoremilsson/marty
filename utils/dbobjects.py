# -*- coding: utf-8 -*-

class Schema(object):

    def __init__(self, oid, name):
        self.oid = oid
        self.name = name

    def __repr__(self):
        return u'<Schema {} ({})>'.format(self.name, self.oid)


class Table(object):

    def __init__(self, con, schema, oid, name):
        self.con = con
        self.schema = schema
        self.oid = oid
        self.name = name
        self.columns = []
        self.update = None

    def __repr__(self):
        return u'<Table {} ({})>'.format(self.name, self.oid)

    @property
    def long_name(self):
        return '{}.{}'.format(self.schema.name, self.name)

    @property
    def internal_name(self):
        return 'data_{}_{}_{}'.format(self.schema.name, self.name, self.update)

    @property
    def internal_columns(self):
        for column in self.columns:
            yield column
        yield StartColumn()
        yield StopColumn()

    def add_column(self, name, number, type, length):
        self.columns.append(Column(self, name, number, type, length))

    def data(self):
        cur = self.con.cursor()
        cur.execute('SELECT * FROM {}'.format(self.long_name))
        for row in cur:
            yield row
        cur.close()


class Column(object):

    def __init__(self, table, name, number, type, length):
        self.table = table
        self.name = name
        self.number = number
        self.type = type
        self.length = length

    def __repr__(self):
        return u'<Column {} {}({})>'.format(self.name, self.type, self.length)

    @property
    def internal_name(self):
        return 'data_{}_{}'.format(self.name, self.table.update)


class StartColumn(object):
    internal_name = 'start'
    type = 'integer REFERENCES marty_updates(id) NOT NULL'
    length = -1


class StopColumn(object):
    internal_name = 'stop'
    type = 'integer REFERENCES marty_updates(id)'
    length = -1

