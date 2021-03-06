import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from collections import OrderedDict

class Row(object):
    """One row from a SQL query."""

    def __init__(self, keys, values):
        # make sure the length of keys and values are equal.
        assert len(keys) == len(values)

        self.keys = keys
        self.values = values

    def get_column_names(self):
        """Return column names."""
        return self.keys
    
    def get_values(self):
        """Return values."""
        return self.values

    def __len__(self):
        return len(self.keys)

    def __getitem__(self, key):
        # Access by index
        if isinstance(key, int):
            assert key < len(self.keys)
            return self.values[key]

        # Access by column name
        if key in self.keys:
            index = self.keys.index(key)
            return self.values[index]

        raise KeyError('Invalid key.')

    def __repr__(self):
        return '<Record {} >'.format(self.as_dict(True))

    def as_dict(self, ordered=False):
        """Merge the keys and values as a dict"""
        if ordered:
            return OrderedDict(zip(self.keys, self.values))
        return dict(zip(self.keys, self.values))

    def dataframe(self):
        """Tranform a row to pandas dataframe."""
        dictionary = OrderedDict(zip(self.keys, [[value] for value in self.values]))
        dataframe = pd.DataFrame(dictionary)
        return dataframe


class RowsCollection(object):
    """A collection of all rows from a SQL query."""

    def __init__(self, rows):
        self.records = rows
        self.all_records = []
        self.pending = True

    def __len__(self):
        return len(self.all_records)

    def __repr__(self):
        return '<RowsCollection length = {} pending = {}>'.format(len(self), self.pending)
    
    def next(self):
        return self.__next__()

    def __next__(self):
        """Consume the underlying generator and return the next row"""
        try:
            next_record = next(self.records)
            self.all_records.append(next_record)
            return next_record
        except StopIteration:
            self.pending = False
            raise StopIteration('At the end of the result set.')

    def __iter__(self):
        """Fetch cached records first."""
        counter = 0
        while True:
            if counter < len(self.all_records):
                yield self.all_records[counter]
            else:
                yield self.next()
            counter += 1

    def fetch_all(self):
        """Return a list of all rows."""
        return list(iter(self))

    def __getitem__(self, key):
        # If the key is a int.
        if isinstance(key, int):
            while key >= len(self.all_records):
                self.next()
            return self.all_records[key]

        while key.stop >= len(self.all_records):
            self.next()
        return RowsCollection(iter(self.all_records[key]))
 
    def dataframe(self):
        """Transform cached rows into pandas dataframe."""
        if not self.all_records:
            print('No rows cached.')
            return
        dict_list = [row.as_dict() for row in self.all_records]
        columns = self.all_records[0].keys
        dataframe = pd.DataFrame(dict_list, columns=columns)
        return dataframe



class Connection(object):
    """ A database connection"""
    
    def __init__(self, url):
        self.db_url = url 

        if not self.db_url:
            raise ValueError('You must provice a database url.')

        self.connected = False
        try:
            self.engine = create_engine(self.db_url)
            self.connection = self.engine.connect()
            self.connected = True
        except Exception as e:
            raise e

    def close(self):
        """ Close database connection """
        self.connection.close()
        self.connected = False

    def get_url(self):
        """Return database uri of the connection."""
        return self.db_url

    def get_table_names(self):
        """Return all table names of the connected database."""
        return self.engine.table_names()

    def query(self, sql):
        """Excecute the given SQL against the database."""
        try:
            res_cursor = self.connection.execute(text(sql))
        except Exception as e:  
            raise e("SQL execution error!")
        
        rows = (Row(res_cursor.keys(), record) for record in res_cursor)
        results = RowsCollection(rows)
        return results
    
