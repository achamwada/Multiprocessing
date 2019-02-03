from threading import Thread
from DBUtils.PooledDB import PooledDB
import pymysql
from pymysql.cursors import DictCursor

class DBPooling():
    def __init__(self, db_thread_instances):
        # Information required to create a connection object
        dbServerIP = "localhost"  # Test

        mySQLPort = 3306

        dbUserName = "alex"  # User name of the MySQL database serve

        dbUserPassword = "test"

        databaseToUse = "operations"  # Name of the MySQL database to be used

        charSet = "utf8"  # Character set

        max_conn = 40

        self.threadID = 0

        self.instances = db_thread_instances

        # Create a PoolDB instance
        self.Db_Pool = PooledDB(creator=pymysql,
                               # Python function returning a connection or a Python module, both based on DB-API 2

                               host=dbServerIP,

                                port=mySQLPort,

                               user=dbUserName,

                               password=dbUserPassword,

                               database=databaseToUse,

                               autocommit=True,

                               charset=charSet,

                               cursorclass=pymysql.cursors.DictCursor,

                               blocking=True,

                               maxconnections=max_conn)


        threadCollection = []
        self.active_conns = 0
        #print("\nNumber of initial active connections " + str(self.Db_Pool._connections))
        for i in range(self.instances):
            self.mySQLConnection = self.Db_Pool.connection()
            self.active_conns = self.active_conns + self.Db_Pool._connections
            cthread = Thread(target=self.tfunc(),args=(i,))
            cthread.start()
            #print("\nNumber of active connections " + str(self.active_conns))

            threadCollection.append(cthread)

        for thread in threadCollection:
            thread.join()

    def tfunc(self, *args):
        #print(args)
        try:
            self.threadID = self.threadID + 1
            #print("\nThread {} started ".format(self.threadID))

        except Exception as e:

            print("Exception: %s" % e)

        return

    def select(self, query):

        try:
            cursor = self.mySQLConnection.cursor()

            # Execute query
            cursor.execute(query)

            # Create a result list
            result_list = cursor.fetchall()

            cursor.close()

            return result_list

        except TypeError as e:
            return "Error: " + str(e)

    # Execute Statement (Insert/Update/Delete)
    def execute(self, query, params=False):

        # Try and execute the query
        try:
            cursor = self.mySQLConnection.cursor()

            # Execute query
            if(params != False):
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Return the last insert ID
            return cursor.lastrowid

        except TypeError as e:
            # return False
            return None

    def __del__(self):
        print("Closing database connection")
        self.mySQLConnection.close()