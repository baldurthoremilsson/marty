# -*- coding: utf-8 -*-

import logging


class HistoryPopulator(object):

    def __init__(self, con, logger=None):
        self.con = con
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
            internal_name name NOT NULL,
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
            internal_name name NOT NULL,
            start integer REFERENCES marty_updates(id) NOT NULL,
            stop integer REFERENCES marty_updates(id)
        )
        """)
        cur.close()
        self.con.commit()

    def update(self):
        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_updates DEFAULT VALUES RETURNING id
        """)
        self._update_id = cur.fetchone()[0]
        cur.close()
        self.con.commit()
        self.logger.debug('new update id {}'.format(self._update_id))

    def add_schema(self, schema):
        self.logger.info('adding schema {}'.format(schema.name))

        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_schemas(oid, name, start) VALUES(%s, %s, %s)
        """, (schema.oid, schema.name, self._update_id))
        cur.close()
        self.con.commit()

        self.logger.debug(cur.query)

    def add_table(self, table):
        self.logger.info('adding table {}'.format(table.long_name))

        update = self._update_id
        table.update = update
        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_tables(oid, name, schema, internal_name, start)
        VALUES(%s, %s, %s, %s, %s)
        """, (table.oid, table.name, table.schema.oid, table.internal_name, self._update_id))
        cur.close()
        self.con.commit()

        self.logger.debug(cur.query)

        for column in table.columns:
            self.add_column(column)

    def add_column(self, column):
        self.logger.info('adding column {} to {}'.format(column.name, column.table.long_name))

        cur = self.con.cursor()
        cur.execute("""
        INSERT INTO marty_columns(table_oid, name, number, type, length, internal_name, start)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        """, (column.table.oid, column.name, column.number, column.type, column.length, column.internal_name, self._update_id))
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
            values.extend([self._update_id, None])
            cur.execute(query, values)

            self.logger.debug(cur.query)

        cur.close()
        self.con.commit()

    def insert(self, table, block, offset, row):
        self.logger.info('inserting to table {}'.format(table.internal_name))
        table_name = table.internal_name
        column_names = ', '.join(column.internal_name for column in table.internal_columns)
        value_list = ', '.join('%s' for column in table.internal_columns)
        query = 'INSERT INTO {}({}) VALUES({})'.format(table_name, column_names, value_list)

        values = ['({},{})'.format(block, offset)] + list(row) + [self._update_id, None]
        cur = self.con.cursor()
        cur.execute(query, values)
        self.logger.debug(cur.query)
        cur.close()
        self.con.commit()

    def delete(self, table, block, offset):
        self.logger.info('deleting from table {}'.format(table.internal_name))
        query = 'UPDATE {} SET stop = %s WHERE data_ctid = %s'.format(table.internal_name)
        values = [self._update_id, '({},{})'.format(block, offset)]
        cur = self.con.cursor()
        cur.execute(query, values)
        self.logger.debug(cur.query)
        cur.close()
        self.con.commit()


class DevPopulator(object):

    def __init__(self, con, update, history_coninfo, logger=None):
        self.con = con
        self.update = update
        self.history_coninfo = history_coninfo
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())

    def initialize(self):
        cur = self.con.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS marty')
        cur.execute('CREATE EXTENSION IF NOT EXISTS dblink')
        cur.execute("""
        CREATE TABLE marty.bookkeeping(
            view_name name UNIQUE,
            local_table name,
            cached boolean DEFAULT false,
            coldef text,
            remote_select_stmt text,
            temp_table_def text
        )
        """)

        cur.execute("""
        CREATE FUNCTION coninfo() RETURNS text AS $$
        BEGIN
            RETURN '{coninfo}';
        END;
        $$ LANGUAGE plpgsql;
        """.format(coninfo=self._dblink_connstr()))

        cur.execute("""
        CREATE FUNCTION view_select(my_view_name text) RETURNS SETOF RECORD AS $$
        DECLARE
            view_info RECORD;
        BEGIN
            SELECT * FROM marty.bookkeeping WHERE view_name = my_view_name INTO view_info;
            IF NOT view_info.cached THEN
                RAISE NOTICE 'fetching %', view_info.view_name;
                EXECUTE ' INSERT INTO ' || view_info.local_table ||
                        ' SELECT ' || view_info.coldef ||
                        ' FROM dblink(''' || coninfo() || ''', ''' || view_info.remote_select_stmt || ''')'
                        ' AS ' || view_info.temp_table_def;
                UPDATE marty.bookkeeping SET cached = true WHERE view_name = my_view_name;
            END IF;
            RETURN QUERY EXECUTE 'SELECT ' || view_info.coldef || ' FROM ' || view_info.local_table;
        END;
        $$ LANGUAGE plpgsql;
        """)
        cur.close()

    def _dblink_connstr(self):
        parts = {
            'host': 'host={}',
            'user': 'user={}',
            'port': 'port={}',
            'database': 'dbname={}',
        }
        return ' '.join(parts[key].format(value) for key, value in self.history_coninfo.iteritems())

    def create_schema(self, schema):
        self.logger.info('Creating schema {}'.format(schema.name))
        cur = self.con.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(schema.name))
        cur.close()

    def create_table(self, table):
        self.logger.info('Creating table {}'.format(table.long_name))

        # Create table for local data
        table.update = self.update
        query = 'CREATE TABLE marty.{table}({cols})'
        cols = ','.join('\n  {name} {type}'.format(name=column.name, type=column.type) for column in table.columns)
        cur = self.con.cursor()
        cur.execute(query.format(table=table.internal_name, cols=cols))
        for column in table.columns:
            cur.execute("""
            UPDATE pg_attribute
            SET atttypmod = %(column_length)s
            WHERE attrelid = %(table_name)s::regclass::oid AND attname = %(column_name)s
            """, {'column_length': column.length, 'table_name': 'marty.{}'.format(table.internal_name), 'column_name': column.name})


        # Create view that combines local and remote data
        my_cols = ', '.join([col.name for col in table.columns])
        temp_columns = ['{name} {type}'.format(name=col.name, type=col.type) for col in table.columns]
        temp_table_def = 't1({columns})'.format(columns=', '.join(temp_columns))

        view_query = """
        CREATE VIEW {view_name}
        AS SELECT {cols} FROM view_select('{view_name}')
        AS {tabledef};
        """
        cur.execute(view_query.format(view_name=table.long_name, cols=my_cols, tabledef=temp_table_def))

        bookkeeping_query = """
        INSERT INTO marty.bookkeeping(view_name, local_table, coldef, remote_select_stmt, temp_table_def)
        VALUES(%(view_name)s, %(local_table)s, %(coldef)s, %(remote_select_stmt)s, %(temp_table_def)s);
        """
        local_cols = ', '.join([col.name for col in table.columns])
        internal_cols = ', '.join([col.internal_name for col in table.columns])
        remote_select_stmt = 'SELECT {cols} FROM {table} WHERE start <= {update} and (stop IS NULL or stop > {update})'
        bookkeeping_values = {
            'view_name': table.long_name,
            'local_table': 'marty.' + table.internal_name,
            'coldef': local_cols,
            'remote_select_stmt': remote_select_stmt.format(cols=internal_cols, table=table.internal_name, update=self.update),
            'temp_table_def': temp_table_def,
        }
        cur.execute(bookkeeping_query, bookkeeping_values)

        trigger_queries_values = {
            'trigger_name': table.long_name.replace('.', '_'),
            'local_table': 'marty.' + table.internal_name,
            'local_columns': my_cols,
            'new_values_insert': ', '.join(['NEW.' + col.name for col in table.columns]),
            'new_values_update': ', '.join(['{name} = NEW.{name}'.format(name=col.name) for col in table.columns]),
            'old_values': ' AND '.join(['{name} = OLD.{name}'.format(name=col.name) for col in table.columns]),
            'view_name': table.long_name,
        }

        # Create insert trigger for view
        insert_query = """
        CREATE FUNCTION {trigger_name}_insert() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {local_table}({local_columns}) VALUES({new_values_insert});
                RETURN NEW;
            END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {trigger_name}_insert_trigger
        INSTEAD OF INSERT ON {view_name}
        FOR EACH ROW EXECUTE PROCEDURE {trigger_name}_insert();
        """
        cur.execute(insert_query.format(**trigger_queries_values))

        # Create update trigger for view
        update_query = """
        CREATE FUNCTION {trigger_name}_update() RETURNS trigger AS $$
            BEGIN
                UPDATE {local_table} SET {new_values_update} WHERE {old_values};
                RETURN NEW;
            END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {trigger_name}_update_trigger
        INSTEAD OF UPDATE ON {view_name}
        FOR EACH ROW EXECUTE PROCEDURE {trigger_name}_update();
        """
        cur.execute(update_query.format(**trigger_queries_values))

        # Create delete trigger for view
        delete_query = """
        CREATE FUNCTION {trigger_name}_delete() RETURNS trigger AS $$
            BEGIN
                DELETE FROM {local_table} WHERE {old_values};
                RETURN OLD;
            END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {trigger_name}_delete_trigger
        INSTEAD OF DELETE ON {view_name}
        FOR EACH ROW EXECUTE PROCEDURE {trigger_name}_delete();
        """
        cur.execute(delete_query.format(**trigger_queries_values))
        cur.close()

