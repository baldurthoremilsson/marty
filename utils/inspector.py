# -*- coding: utf-8 -*-

from dbobjects import Schema, Table, Column, StartColumn, StopColumn

class SlaveInspector(object):

    def __init__(self, con, logger=None):
        self.con = con
        self.db_oid = self._get_db_oid()
        self.tabledict = {}
        self._system_tables = None
        self.pg_namespace = None
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())

    def _get_db_oid(self):
        cur = self.con.cursor()
        cur.execute('SELECT oid FROM pg_database WHERE datname = current_database()')
        row = cur.fetchone()
        cur.close()
        return row[0]

    def schemas(self):
        cur = self.con.cursor()
        cur.execute("""
        SELECT ctid, oid, nspname
        FROM pg_namespace
        WHERE nspname NOT LIKE 'information_schema' AND nspname NOT LIKE 'pg_%'
        """)
        for ctid, oid, name in cur:
            self.logger.info('schema {}, {}, {}'.format(ctid, oid, name))
            yield Schema(ctid, oid, name)
        cur.close()
        self.con.commit()

    def tables(self, schema):
        """
        Missing:
            indexes (relkind = i)
            sequences (relkind = S)
            views (relkind = v)
            materialized views (relkind = m)
            composite type (relkind = c)
            TOAST tables (relkind = t)
            foreign tables (relkind = f)
        """
        cur = self.con.cursor()
        cur.execute("""
        SELECT ctid, oid, relname, pg_catalog.pg_relation_filenode(oid) AS filenode
        FROM pg_class
        WHERE relnamespace = %s AND relkind = 'r'
        """, (schema.oid,))
        for ctid, oid, name, filenode in cur:
            self.logger.info('table {}, {} ({})'.format(oid, name, filenode))
            table = Table(schema, ctid, oid, name, con=self.con)
            self.tabledict[filenode] = table
            yield table
        cur.close()
        self.con.commit()

    def columns(self, table):
        """
        Missing:
            arrays (attndims)
            data in TOAST tables (attstorage)
            not null (attnotnull)
            default value (atthasdef)
            attislocal?
            attinhcount?
            collation (attcollation)
            attoptions?
            attfdwoptions?
        """
        cur = self.con.cursor()
        cur.execute("""
        SELECT pg_attribute.ctid, attname, attnum, typname, atttypmod
        FROM pg_attribute
        LEFT JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
        WHERE attrelid = %s AND attisdropped = false AND attnum > 0
        ORDER BY attnum ASC
        """, (table.oid,))
        for ctid, name, number, type, length in cur:
            self.logger.info('column {} {}({})'.format(name, type, length))
            table.add_column(ctid, name, number, type, length)
        cur.close()
        self.con.commit()

    @property
    def system_tables(self):
        """
        This looks up tables
            pg_namespace
            pg_class
        """
        if self._system_tables == None:
            self._system_tables = {}
            schema = Schema(None, None, 'pg_catalog')
            cur = self.con.cursor()
            cur.execute("""
            SELECT ctid, oid, relname, pg_catalog.pg_relation_filenode(oid) as filenode
            FROM pg_class
            WHERE relname IN ('pg_namespace', 'pg_class', 'pg_attribute')
            """)
            for ctid, oid, name, filenode in cur:
                self.logger.info('system table {}, {} ({})'.format(oid, name, filenode))
                table = Table(schema, ctid, oid, name, con=self.con)
                self._system_tables[filenode] = table
            cur.close()
            self.con.commit()
        return self._system_tables

    def get_schema(self, ctid=None, oid=None):
        query = 'SELECT ctid, oid, nspname FROM pg_namespace '
        if oid:
            query += 'WHERE oid = %s'
            values = (oid,)
        else:
            query += 'WHERE ctid = %s'
            values = (ctid,)
        cur = self.con.cursor()
        cur.execute(query, values)
        ctid, oid, nspname = cur.fetchone()
        cur.close()
        self.con.commit()
        return Schema(ctid, oid, nspname)

    def get_table(self, ctid=None, oid=None):
        query = 'SELECT ctid, oid, relname, relnamespace, relkind FROM pg_class WHERE relkind = %s AND '
        values = ['r']
        if oid:
            query += 'oid = %s'
            values.append(oid)
        else:
            query += 'ctid = %s'
            values.append(ctid)
        cur = self.con.cursor()
        cur.execute(query, values)
        ctid, oid, relname, relnamespace, relkind = cur.fetchone()
        cur.close()
        self.con.commit()
        schema = self.get_schema(oid=relnamespace)
        return Table(schema, ctid, oid, relname)

    def get_column(self, ctid=None, oid=None, update=None, internal_name=None):
        query = """
        SELECT pg_attribute.ctid, attrelid, attname, attnum, typname, atttypmod
        FROM pg_attribute
        LEFT JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
        WHERE %s AND attisdropped = false AND attnum > 0
        ORDER BY attnum ASC
        """
        if oid:
            query %= 'attrelid = %s'
            values = (oid,)
        else:
            query %= 'pg_attribute.ctid = %s'
            values = (ctid,)
        cur = self.con.cursor()
        cur.execute(query, values)
        row = cur.fetchone()
        if not row:
            return None
        ctid, attrelid, attname, attnum, typname, atttypmod = row
        cur.close()
        self.con.commit()
        table = self.get_table(oid=attrelid)
        table.update = update
        return Column(table, ctid, attname, attnum, typname, atttypmod, internal_name=internal_name)

    def resume(self):
        self.logger.info('resuming')
        cur = self.con.cursor()
        cur.execute('SELECT pg_xlog_replay_resume()')
        cur.close()
        self.con.commit()

    def get(self, table, block, offset, cols=None):
        cur = self.con.cursor()
        if cols:
            cols = ', '.join(cols)
        else:
            cols = '*'
        query = "SELECT {} FROM {} WHERE ctid = '({},{})'"
        query = query.format(cols, table.long_name, block, offset)
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
        self.con.commit()
        return row


class HistoryInspector(object):

    def __init__(self, con, logger=None):
        self.con = con
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())
        self.update = self._update()

    def _update(self):
        cur = self.con.cursor()
        cur.execute("""
        SELECT id, time
        FROM marty_updates
        ORDER BY time DESC LIMIT 1
        """)
        update_id, time = cur.fetchone()
        cur.close()
        self.logger.debug('got update id {} from {}'.format(update_id, time))
        return update_id

    def schemas(self):
        cur = self.con.cursor()
        cur.execute("""
        SELECT _ctid, oid, name
        FROM marty_schemas
        WHERE start <= %(update_id)s AND (stop IS NULL OR stop > %(update_id)s)
        """, {'update_id': self.update})
        for ctid, oid, name, in cur:
            yield Schema(ctid, oid, name)
        cur.close()

    def tables(self, schema):
        cur = self.con.cursor()
        cur.execute("""
        SELECT oid, name, internal_name
        FROM marty_tables
        WHERE schema = %(schema_id)s
          AND start <= %(update_id)s AND (stop IS NULL OR stop > %(update_id)s)
        """, {'schema_id': schema.oid, 'update_id': self.update})
        for oid, name, internal_name in cur:
            yield Table(schema, oid, name, internal_name=internal_name)
        cur.close()

    def columns(self, table):
        cur = self.con.cursor()
        cur.execute("""
        SELECT name, number, type, length, internal_name
        FROM marty_columns
        WHERE table_oid = %(table_oid)s
          AND start <= %(update_id)s AND (stop IS NULL OR stop > %(update_id)s)
        ORDER BY number ASC
        """, {'table_oid': table.oid, 'update_id': self.update})
        for name, number, type, length, internal_name in cur:
            table.add_column(name, number, type, length, internal_name=internal_name)
        cur.close()

