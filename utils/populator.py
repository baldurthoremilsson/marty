# -*- coding: utf-8 -*-

class Populator(object):

    def __init__(self, con, logger=None):
        self.con = con
        self._lock_update = False
        self._update_id = None
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())

    def create_tables(self):
        self.logger.info('creating tables')

        cur = self.con.cursor()
        # marty_updates
        cur.execute("""
        CREATE TABLE IF NOT EXISTS marty_updates(
            id SERIAL PRIMARY KEY,
            time TIMESTAMP DEFAULT current_timestamp NOT NULL
        )
        """)

        # marty_schemas
        cur.execute("""
        CREATE TABLE IF NOT EXISTS marty_schemas(
            oid oid NOT NULL,
            name name NOT NULL,
            start integer REFERENCES marty_updates(id) NOT NULL,
            stop integer REFERENCES marty_updates(id)
        )
        """)

        # marty_tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS marty_tables(
            oid oid NOT NULL,
            name name NOT NULL,
            schema oid NOT NULL,
            data_table name NOT NULL,
            start integer REFERENCES marty_updates(id) NOT NULL,
            stop integer REFERENCES marty_updates(id)
        )
        """)

        # marty_columns
        cur.execute("""
        CREATE TABLE IF NOT EXISTS marty_columns(
            table_oid oid NOT NULL,
            name name NOT NULL,
            number int2 NOT NULL,
            type name NOT NULL,
            length int4 NOT NULL,
            data_column name NOT NULL,
            start integer REFERENCES marty_updates(id) NOT NULL,
            stop integer REFERENCES marty_updates(id)
        )
        """)
        cur.close()
        self.con.commit()

    def lock_update(self):
        self._lock_update = True

    def unlock_update(self):
        self._lock_update = False

    @property
    def _update(self):
        if self._update_id != None and self._lock_update:
            return self._update_id

        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_updates DEFAULT VALUES RETURNING id
        """)
        self._update_id = cur.fetchone()[0]
        cur.close()
        self.con.commit()
        self.logger.debug('new update id {}'.format(self._update_id))
        return self._update_id

    def add_schema(self, schema):
        self.logger.info('adding schema {}'.format(schema.name))

        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_schemas(oid, name, start) VALUES(%s, %s, %s)
        """, (schema.oid, schema.name, self._update))
        cur.close()
        self.con.commit()

        self.logger.debug(cur.query)

    def add_table(self, table):
        self.logger.info('adding table {}'.format(table.long_name))

        update = self._update
        table.update = update
        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_tables(oid, name, schema, data_table, start)
        VALUES(%s, %s, %s, %s, %s)
        """, (table.oid, table.name, table.schema.oid, table.internal_name, self._update))
        cur.close()
        self.con.commit()

        self.logger.debug(cur.query)

        for column in table.columns:
            self.add_column(column)

    def add_column(self, column):
        self.logger.info('adding column {} to {}'.format(column.name, column.table.long_name))

        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_columns(table_oid, name, number, type, length, data_column, start)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        """, (column.table.oid, column.name, column.number, column.type, column.length, column.internal_name, self._update))
        cur.close()
        self.con.commit()

        self.logger.debug(cur.query)

    def create_table(self, table):
        self.logger.info('creating table {}'.format(table.internal_name))

        cur = self.con.cursor()
        cols = ','.join('\n  {} {}'.format(column.internal_name, column.type) for column in table.internal_columns)
        cur.execute('CREATE TABLE {}({})'.format(table.internal_name, cols))

        self.logger.debug(cur.query)

        for column in table.columns:
            cur.execute("""
            UPDATE pg_attribute
            SET atttypmod = %s
            WHERE attrelid = %s AND attname = %s
            """, (column.length, column.table.oid, column.name))
        cur.close()
        self.con.commit()

    def fill_table(self, table):
        self.logger.info('filling table {}'.format(table.internal_name))

        table_name = table.internal_name
        column_names = ', '.join(column.internal_name for column in table.internal_columns)
        value_list = ', '.join('%s' for column in table.internal_columns)
        query = 'INSERT INTO {}({}) VALUES({})'.format(table_name, column_names, value_list)

        cur = self.con.cursor()
        for line in table.data():
            values = list(line)
            values.extend([self._update, None])
            cur.execute(query, values)

            self.logger.debug(cur.query)

        cur.close()
        self.con.commit()

