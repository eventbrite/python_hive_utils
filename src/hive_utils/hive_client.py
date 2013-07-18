import contextlib

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from hive_metastore.ttypes import FieldSchema
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class HiveClientException(Exception):
    pass


@contextlib.contextmanager
def openclose(transport):
    if not getattr(transport, 'keep_open', None):
        transport.open()
    yield
    if not getattr(transport, 'keep_open', None):
        transport.close()


class HiveClient(object):
    """A wrapper around the thrift API."""

    def __init__(self, server='localhost', port=10001, db='default'):
        """Initialize the Hive Client.

        :parameter server(string): server to connect to. Default- localhost
        :parameter port(int): port to connect to. Default- 10000
        :parameter db(string): databased name. Default- default

        :return: None

        """
        transport = TSocket.TSocket(server, port)
        self.__transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(self.__transport)

        self.__client = ThriftHive.Client(protocol)

        self.__db = db

        # make sure this DB exists!
        with openclose(self.__transport):
            assert self.__client.get_database(db)


    def execute(self, *args, **kwargs):
        """Execute a HiveQL command.

        Returns a generator which pulls in a buffered manner

        """
        with openclose(self.__transport):
            self.__client.execute(*args, **kwargs)

            # get names for the fields
            schema = self.__client.getThriftSchema()
            stypes = []
            for schema in schema.fieldSchemas:
                stypes.append(schema.name)


            while True:
                # used buffered read, thrift no likey big reads
                rows = self.__client.fetchN(500)

                if not len(rows):
                    break

                for row in rows:
                    # map names to values.  ordering is not preserved
                    yield dict(zip(stypes,row.split('\t')))


    @contextlib.contextmanager
    def session(self):
        """Creates a session during which the connection remains open.

        Use this for a contiguous block of Hive commands.

        """
        self.__transport.open()
        self.__transport.keep_open = True
        yield
        self.__transport.close()
        self.__transport.keep_open = False

    def get_table(self, table_name):
        """Get hive metastore Table object"""
        with openclose(self.__transport):
            try:
                table = self.__client.get_table(self.__db, table_name)
                return table
            except:
                raise HiveClientException('Table %s does not exist' %
                                        (table_name))

    def get_columns(self, table_name):
        """Get ordered columns list for a table.

        :parameter table_name(string): name of the table.

        :return: list of tuples (name, data_type)

        """
        with openclose(self.__transport):
            table = self.__client.get_table(self.__db, table_name)
            columns = [(getattr(col, 'name'), getattr(col, 'type'))
                       for col in table.sd.cols]

            return columns

    def add_column(self, table_name, column_name, data_type, comment=''):
        """Add a column to the table, appending it at the end.

        :parameter table_name(string): name of the table.
        :parameter column_name(string): name of column to add.
        :parameter data_type(string): data type for the new column (text, int,
                etc.)
        :parameter comment(string): optional comment for the column desc.

        :return: None

        :raises HiveClientException: if the column already exists.

        """
        with openclose(self.__transport):
            table = self.__client.get_table(self.__db, table_name)
            if column_name in table.sd.cols:
                raise HiveClientException('Table %s already has column %s',
                        table_name, column_name)
            new_column = FieldSchema(name=column_name, type=data_type,
                                     comment=comment)
            table.sd.cols.append(new_column)
            self.__client.alter_table(self.__db, table_name, table)

    def remove_column(self, table_name, column_name):
        """Remove a column from the table.

        If the column does not exist in the table, this has no effect.

        :parameter table_name(string): name of the table.
        :parameter column_name(string): name of the column to remove.

        :return: None

        :raises HiveClientException: if the column doesn't exist in the table.

        """
        with openclose(self.__transport):
            table = self.__client.get_table(self.__db, table_name)
            num_cols = len(table.sd.cols)
            table.sd.cols = [col for col in table.sd.cols
                             if col.name != column_name]

            if (len(table.sd.cols) - 1) == num_cols:
                self.__client.alter_table(self.__db, table_name, table)
            else:
                raise HiveClientException('Table %s does not have column %s' %
                        (table_name, column_name))

    def alter_column_type(self, table_name, column_name, data_type, comment=''):
        """Alter the column type of a table.

        If the column type is correct, this has no effect.

        :parameter table_name(string): name of the table.
        :parameter column_name(string): name of column to add.
        :parameter data_type(string): data type for the column (text, int,
                etc.)

        :return: None

        :raises HiveClientException: if the column doesn't exist in the table.

        """
        with openclose(self.__transport):
            table = self.__client.get_table(self.__db, table_name)
            col_names = [getattr(col, 'name')
                         for col in table.sd.cols]
            if column_name not in col_names:
                raise HiveClientException('Table %s does not have column %s',
                        table_name, column_name)
            columns_to_alter = [col for col in table.sd.cols
                                if col.name == column_name and
                                col.type != data_type]
            for column_to_alter in columns_to_alter:
                column_to_alter.type = data_type
                self.__client.alter_table(self.__db, table_name, table)
