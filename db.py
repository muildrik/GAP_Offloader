try:
    import sys, os, pymssql, codecs, random, time
    import socket, urllib3, requests, inspect
    from json import JSONDecodeError
    from socket import gaierror
    from multiprocessing import Queue
    from multiprocessing.pool import ThreadPool
    from pymssql import OperationalError, InterfaceError, DatabaseError, ProgrammingError
    from datetime import datetime, date
    from tkinter import filedialog, messagebox
    from tinydb import TinyDB, where, Query
    from tinydb.storages import JSONStorage
    from tinydb.middlewares import CachingMiddleware
except ImportError as error:
    print(error)

class DB:
    """ This class controls TMS database interactions
    ## Methods (31)
    - setLogger : logger (Logger) -> None
    - setErrorHandler : errorHandler (ErrorHandler) -> None
    - outputToConsole : pbar (function), outputToConsole (function) -> None
    - setHost : host (str) -> None 
    - setName : name (str) -> None
    - setUser : user (str) -> None
    - setScratchTable : table (str) -> None
    - setDBUpdate : flag (str) -> None
    - askQuestion : askQuestion (askQuestion) -> None
    - notifyUser : notify (bool) -> None
    - verification : verifyConnection (function) -> None
    - checkTinyDB : () -> None
    - removeTinyDB : () -> None
    - getDB : () -> None
    - checkAccess : password (str) -> None
    - dropScratchTable : () -> None
    - makeScratchTable : () -> None
    - queryBuilder : query (str), param (str) -> None
    - getMMIDs : verification (bool) -> str
    - getMTIDs : () -> str
    - getIIDs : () -> str
    - getAllMMIDS : () -> None
    - getAllMTIDS : () -> None
    - getAllIIDS : () -> None
    - find : table (str), field (str), id (str) -> list
    - count : table (str), field (str), id (str) -> int
    - addMediaFiles : records (list) -> None
    - updateMediaRenditions : records (list) -> None
    - connect : idx (int) -> None
    - close : idx (int) -> None
    - commit : idx (int) -> None
    """

    def __init__(self):
        self.port = 1433
        self.connections = {}
        self.usedStrings = []
        self.items = []
        self.mmids = []
        self.mtids = []
        self.dbPath = os.path.join(os.getcwd(), 'offloader.json')

    def setLogger(self, logger) -> None:
        """
        Assign the logger to this class

        Args:
            logger (Logger): logger instance
        """
        self.logger = logger

    def setErrorHandler(self, errorHandler) -> None:
        """ Assign the errorHandler to this class 
        ### Parameters
        - errorHandler (ErrorHandler) : ErrorHandler instance
        """
        self.errorHandler = errorHandler

    def outputToConsole(self, pbar:function, outputToConsole:function) -> None:
        """ Assign the outputToConsole method to this class 
        ### Parameters
        - pbar (function) : updatePBar method
        - outputToConsole (function) : outputToConsole method
        """
        self.updatePBar = pbar
        self.output = outputToConsole

    def setHost(self, host:str) -> None:
        """ Set the host on this class
        ### Parameters
        - host (str) : host
        """
        self.host = host

    def setName(self, name:str) -> None:
        """ Set the name of the database on this class
        ### Parameters
        - name (str) : database name
        """
        self.db = name

    def setUser(self, user:str) -> None:
        """ Set the name of the database on this class
        ### Parameters
        - user (str) : user name
        """
        self.user = user

    def setScratchTable(self, table):
        """ Set the name of the scratch table
        ### Parameters
        - table (str) : name of the temporary table
        """
        self.tempTable = table

    def setDBUpdate(self, flag:bool) -> None:
        """ Update the TMS database or not
        ### Parameters
        - flag (bool) : controls if updates are pushed to the TMS database
        """
        self.update = flag

    def askQuestion(self, askQuestion:function) -> None:
        """ Binds the askQuestion method to this class
        ### Parameters
        - askQuestion (function) : askQuestion method
        """
        self.ask = askQuestion

    def notifyUser(self, notify:function) -> None:
        """ Binds the notify method to this class
        ### Parameters
        - notify (function) : notify method
        """
        self.notify = notify
    
    def verification(self, verifyConnection:function) -> None:
        """ Binds the verifyConnection method to this class
        ### Parameters
        - verifyConnection (function) : verifyConnection method
        """
        self.verify = verifyConnection

    def checkTinyDB(self) -> None:
        """ Read the TinyDB instance to verify it works. The result will be presented to the user via the notify method. """
        try:
            if os.path.isfile(self.dbPath): # TinyDB exists
                if self.getDB(): # Attempt to read; remove if exception
                    os.remove(self.dbPath)
                    self.getDB()
                    res = {}
                    res['res'] = None
                    res['msg'] = 'Local scratch table was corrupted and had to be refreshed'
                    self.notify(res)
            else:
                self.getDB()
        except OSError as error:
            self.errorHandler.handle(error)

    def removeTinyDB(self) -> None:
        """ Delete the TinyDB method """
        try:
            if os.path.isfile(self.dbPath): os.remove(self.dbPath)
        except OSError as error:
            self.errorHandler.handle(error)


    def getDB(self, table:str="MMIDS") -> None:
        """ Load and set the TinyDB instance on this class
        ### Parameters
        - table (str) : table name for TinyDB (default MMIDS)
        """
        try: 
            self.tinyDB = TinyDB(self.dbPath, default_table=table, sort_keys=True, indent=4, separators=(',', ': '))
            return False
        except :
            return True
        
    def checkAccess(self, password:str) -> None:
        """ Check if the application has access to the TMS server. Variables are set on the class whereby only the password has to be passed to this function.
        ### Parameters
        - password (str) : password to access TMS
        """
        self.password = password
        try: 
            localhost = socket.gethostbyname(socket.gethostname())
            self.outputToConsole(f'DB.CHECKACCESS: Localhost is {localhost}')
            self.updatePBar()
            
            exthost = socket.getaddrinfo(self.host, self.port)
            exthost = exthost[0][4][0]
            self.outputToConsole(f'DB.CHECKACCESS: External host resolved to {exthost}')
            self.updatePBar()
            
            try:
                self.outputToConsole(f'DB.CHECKACCESS: Attempting to connect to {exthost} on port number {self.port}; this may take a moment...')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # portResult = sock.connect_ex((exthost, self.port))

                if sock.connect_ex((exthost, self.port)) == 0:
                    self.outputToConsole(f'DB.CHECKACCESS: Host {self.host} ({exthost}) is available on port number {self.port}')
                    self.updatePBar()
                    self.outputToConsole(f'DB.CHECKACCESS: Establishing connection to {self.host} ({exthost}) on port number {self.port}. This may take a minute...')
                    if self.dropScratchTable():
                        self.outputToConsole(f'DB.CHECKACCESS: Scratch table "{self.tempTable}" dropped')
                        if self.makeScratchTable(): 
                            self.prepareNewMediaExtension()
                            self.outputToConsole(f'DB.CHECKACCESS: Prepared new scratch table "{self.tempTable}"')
                            self.verify(True)
                        else:
                            self.outputToConsole(f'DB.CHECKACCESS: Could not prepare new scratch table "{self.tempTable}"')
                            self.verify(False)
                    else:
                        self.outputToConsole(f'DB.CHECKACCESS: Could not drop scratch table "{self.tempTable}"')
                        self.verify(False)
                else:
                    return self.errorHandler.error('ConnectionError')
            except OSError as error:
                return self.errorHandler.handle(error)
        except gaierror as error:
            return self.errorHandler.handle(error, gaierror)

    def dropScratchTable(self) -> None:
        """ Drop the scratch table in the TMS table on the MS Server """
        self.updatePBar()
        try:
            return self.queryBuilder(f'DROP TABLE IF EXISTS {self.tempTable}')
        except (pymssql.Error, AssertionError, AttributeError, OperationalError, ProgrammingError) as error:
            return self.errorHandler.handle(error)
    
    def makeScratchTable(self) -> None:
        """ Make the scratch table in the TMS table on the MS Server """
        self.updatePBar()
        try:
            return self.queryBuilder(f"IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'dbo.[{self.tempTable}]') AND OBJECTPROPERTY(id, N'IsTable') = 1) BEGIN CREATE TABLE {self.tempTable} (RenditionID INT, FileID INT) END")
        except (AttributeError, AssertionError) as error:
            return self.errorHandler.handle(error)

    def prepareNewMediaExtension(self) -> None:
        """ Add the NRS Media Extension (http://nrs.harvard.edu) in the TMS table on the MS Server """
        self.queryBuilder(f"INSERT IGNORE INTO MediaExtensions (ExtensionID, FormatID, Extension, LoginID, EnteredDate) VALUES(44, 45, 0, '', 'offloader', datetime.now())")

    def getRandInt(self) -> None:
        """ Get a random integer
        ### Returns
        - int : Random integer between 1-100000
        """
        return random.randint(1, 100000)

    def queryBuilder(self, query:str, param:list=None) -> None:
        """ Builds a SQL query to run on the MS Server. The query is supplied, modified based on param, and prepared for commit.
        ### Parameters
        - query (str) : SQL query
        - param (list) : List of parameters to update the SQL query (default=None)
        """
        try:
            connID = self.getRandInt()
            while connID not in self.usedStrings:
                self.usedStrings.append(connID)
                cur = self.cursor(connID)
                if type(param) is list:
                    cur.executemany(query, param)
                    if 'INSERT' in query or 'UPDATE' in query:
                        self.commit(connID)
                        return True
                else:
                    cur.execute(query, param)
                    if 'DROP' in query or 'CREATE' in query:
                        self.commit(connID)
                        return True
                try: 
                    row = cur.fetchall()
                    self.close(connID)
                    return row
                except:
                    self.close(connID)
                    # return None
        except (InterfaceError, OperationalError, DatabaseError, ProgrammingError) as error:
            return self.errorHandler.handle(error)

    def getMMIDs(self, verification:bool=None) -> str:
        """ Downloading MediaRenditions table from MS Server and store it in TinyDB.
        ### Parameters
        - verification (bool) : controls output to user when this method is called for a second time in the process (default=None)
        ### Returns
        - str : updateItems to start the next step in the processing stage
        """
        try:
            table = self.tinyDB.table('MMIDS')
            self.tinyDB.purge_table('MMIDS')
            if verification:
                self.outputToConsole(">>>>>> VERIFYING UPDATES <<<<<<", True)
                self.outputToConsole("OFFLOADER: Reacquiring MediaRenditions...")
            self.outputToConsole(f'DB.GETMMIDS: Cloning MediaRenditions table...')
            mmids = self.queryBuilder(f'SELECT RenditionNumber, MediaMasterID, RenditionID, PrimaryFileID FROM MediaRenditions')
            table.insert_multiple({ 'RenditionNumber' : s["RenditionNumber"], 'MediaMasterID' : s["MediaMasterID"], 'RenditionID' : s["RenditionID"], 'PrimaryFileID' : s["PrimaryFileID"] } for s in mmids)
            return 'updateItems' if verification is None else 'verified'
        except (JSONDecodeError, OSError, ValueError) as error:
            return self.errorHandler.handle(error)

    def getMTIDs(self) -> str:
        """ Downloading MediaFormats table from MS Server and store it in TinyDB.
        ### Returns
        - str : getMMIDs to start the next step in the processing stage
        """
        try:
            table = self.tinyDB.table('MTIDS') 
            self.tinyDB.purge_table('MTIDS')
            self.outputToConsole(f'DB.GETMTIDS: Cloning MediaFormats table...')
            mtids = self.queryBuilder("SELECT * FROM MediaFormats WHERE Format LIKE %s", "JPEG2000")
            table.insert_multiple({ 'Format' : t["Format"], 'FormatID' : t["FormatID"], "MediaTypeID" : t["MediaTypeID"] } for t in mtids)
            return 'getMMIDs'
        except OSError as error:
            return self.errorHandler.handle(error)

    def getIIDs(self) -> str:
        """ Downloading ScratchTable from MS Server and store it in TinyDB.
        ### Returns
        - str : batchUpdate to start the next step in the processing stage
        """
        try:
            table = self.tinyDB.table('IIDS')
            self.tinyDB.purge_table('IIDS')
            # self.outputToConsole(f'DB.{self.getIIDs.__name__.upper()}: Refreshing local table "IIDS" in {self.dbPath}')
            self.outputToConsole(f'DB.GETIIDS: Cloning {self.tempTable} table...')
            iids = self.queryBuilder(f'SELECT RenditionID, FileID FROM {self.tempTable}')
            table.insert_multiple({ 'RenditionID' : t["RenditionID"], 'FileID' : t["FileID"] } for t in iids)
            return 'batchUpdate'
        except OSError as error:
            return self.errorHandler.handle(error)

    def getAllMMIDS(self) -> list: return self.tinyDB.table('MMIDS').all()
    def getAllMTIDS(self) -> list: return self.tinyDB.table('MTIDS').all()
    def getAllIIDS(self) -> list: return self.tinyDB.table('IIDS').all()

    def find(self, table, field, id) -> list: 
        """ Find value in TinyDB
        ### Parameters
        - table (str) : name of the table
        - field (str) : name of the field
        - id (str) : name of the query parameter
        ### Returns
        - list[documents] : list with TinyDB documents
        """
        try: 
            return self.tinyDB.table(table).search(where(field) == id)
        except JSONDecodeError:
            res = {}
            res['res'] = False
            res['msg'] = 'Local scratch table was corrupted and has to be refreshed. Please restart the program.'
            self.notify(res)

    def count(self, table, field, id) -> int:
        """ Count values in TinyDB
        ### Parameters
        - table (str) : name of the table
        - field (str) : name of the field
        - id (str) : name of the query parameter
        ### Returns
        - int : number of relevant TinyDB documents
        """
        return self.tinyDB.table(table).count(where(field) == id)
    
    def addMediaFiles(self, records:list) -> str:
        """ Add new MediaFile records to the MediaFiles table
        ### Parameters
        - records (list) : list with new records to be added
        ### Returns
        - str : getIIDs to start the next step in the processing stage
        """
        try:
            self.queryBuilder(f"INSERT INTO MediaFiles (RenditionID, PathID, FileName, FormatID, LoginID, ArchIDNum, EnteredDate) OUTPUT INSERTED.[RenditionID], INSERTED.[FileID] INTO dbo.{self.tempTable} VALUES(%d, %d, %s, %d, %s, %s, %d)", records)
            return 'getIIDs'
        except:
            print('error')

    def updateMediaRenditions(self, records:list) -> str:
        """ Updates the MediaRenditions table to set new MediaFile for thumbnails
        ### Parameters
        - records (list) : list with new records to be updated
        ### Returns
        - str : verify to start the next step in the processing stage
        """
        try:
            self.queryBuilder(f"UPDATE MediaRenditions SET PrimaryFileID = %s, ThumbPathID = %s, ThumbFileName = %s, ThumbExtensionID = %s WHERE MediaMasterID = %s", records)
            return 'verify'
        except:
            print('error')

    def connect(self, idx:int) -> None:
        """ Sets up a new database connection to the MS Server on the class
        ### Parameters
        - idx (int) : random number for connection
        """
        try:
            cnx = f'connection{idx}'
            conn = pymssql.connect(self.host, self.user, self.password, self.db)
            setattr(self, cnx, conn)
        except InterfaceError as error: return self.errorHandler.handle(error, 'MSSQLDriverException')
        except DatabaseError as error: return self.errorHandler.handle(error, 'MSSQLDatabaseException')
    
    def cursor(self, idx:int) -> None:
        """ Opens a database connection to the MS Server on the class
        ### Parameters
        - idx (int) : random number for connection
        ### Returns
        - Any : pointer to the pymssql cursor on the class
        """
        try:
            self.connect(idx)
            
            cnx = f'connection{idx}'
            cid = f'cursor{idx}'

            conn = getattr(self, cnx)
            
            cur = conn.cursor(as_dict=True)
            setattr(self, cid, cur)

            return getattr(self, cid)
        except (AssertionError, OperationalError) as error:
            return self.errorHandler.handle(error)

    def close(self, idx:int) -> None:
        """ Closes and removes a database connection to the MS Server on the class
        ### Parameters
        - idx (int) : random number for connection
        """
        cursorID = f'cursor{idx}'
        cursor = getattr(self, cursorID)
        cursor.close()
        cnx = f'connection{idx}'
        conn = getattr(self, cnx)
        conn.close()

    def commit(self, idx:int) -> None:
        """ Commits data assigned to a database connection
        ### Parameters
        - idx (int) : random number for connection
        """
        cnx = f'connection{idx}'
        connection = getattr(self, cnx)
        connection.commit()