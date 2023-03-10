import os
from datetime import datetime

path = os.getcwd()
logFile = None

class Logger:
    """ This class controls logging messages and writing them to file.
    ## Methods (11)
    - outputToConsole : outputToConsole (function) -> None
    - setErrorHandler : errorHandler (ErrorHandler) -> None
    - shouldWeLog : log (bool) -> None
    - setLogStartTime : start (datetime) -> None
    - setLog : file (str) -> None
    - setErrorLog : file (str) -> None
    - startSystemLog : file (str) -> None
    - logToSystemLog : entry (str) -> None
    - newLog : logFile (str) -> None
    - log : entry (str) -> None
    - logError : entry (str) -> None
    """

    def __init__(self):
        self.start = 0
        self.logging = False
        self.logFile = None
        self.systemLogFile = None
        self.newLogFile = None
        self.errorLog = None
        self.errCounter = 0      # ERROR COUNTER
        self.logPath = os.path.join(os.getcwd(), 'logs')

    def outputToConsole(self, outputToConsole:function) -> None:
        """ Assign the outputToConsole method to this class 
        ### Parameters
        - outputToConsole (function) : outputToConsole method
        """
        self.output = outputToConsole

    def setErrorHandler(self, errorHandler) -> None:
        """ Assign the errorHandler to this class 
        ### Parameters
        - errorHandler (ErrorHandler) : ErrorHandler instance
        """
        self.errorHandler = errorHandler

    def shouldWeLog(self, log:bool) -> None:
        """ Enable/disable logging
        ### Parameters
        - log (bool) : to log (true) or not (false)
        """
        if log: 
            self.logging = True
            self.logToSystemLog(f'You will have a log for each DRS report')
        else:
            self.logToSystemLog(f'You will not have a log for each DRS report')

    def setLogStartTime(self, start:datetime) -> None:
        """ Method that assigns start time
        ### Parameters
        - start (datetime) : time of starting processing
        """
        self.start = start

    def setLog(self, file:str) -> None:
        """ Method that starts a log file and sets it at default on the class
        ### Parameters
        - file (str) : file name for the log
        """
        file = file.split("\\")[-1]
        self.errorLog = os.path.join(self.logPath, f'{file}.log')

    def setErrorLog(self, file:str):
        """ Method that starts an error log file and sets it at default on the class
        ### Parameters
        - file (str) : file name for the error log
        """
        file = file.split("\\")[-1]
        self.errorLog = os.path.join(self.logPath, f'{file}.error')

    def startSystemLog(self, file:str) -> None:
        """ Method that starts a system log file and sets it at default on the class
        ### Parameters
        - file (str) : file name for the log
        """
        # Create folder if self.logPath does not exist
        if not os.path.isdir(self.logPath): os.mkdir(self.logPath) 
        
        # Check folder for write access
        if os.access(self.logPath, os.W_OK):
            self.systemLogFile = os.path.join(self.logPath, file)
    
            # Delete optional old log file with same name if exists
            if os.path.isfile(self.systemLogFile): os.remove(self.systemLogFile)
    
            self.logToSystemLog(f'Program started ({self.start.strftime("%A, %d. %B %Y %I:%M%p")})')
            self.logToSystemLog(f'System log generated ({datetime.now().strftime("%A, %d. %B %Y %I:%M%p")})')
        else:
            self.output(f'LOGGER.STARTSYSTEMLOG: No write access to the log folder. We will not have a system log!', False)
            self.systemLogFile = None

    def logToSystemLog(self, entry) -> None:
        """ Method that logs an entry to the system log
        ### Parameters
        - entry (str) : entry for the system log
        """
        if self.systemLogFile is not None:
            try:
                file = open(self.systemLogFile, "a", encoding="utf8")
                file.write(datetime.now().strftime("%H:%M:%S.%f") + '\t' + entry + '\n')
                file.close()
            except OSError as error:
                self.errorHandler.handle(error)

    def newLog(self, logFile:str) -> None:
        """ Start .log- and .errors-files for each DRS report to be processed.
        ### Parameters
        - logFile (str) : name for the new log files
        """
        # Create folder if self.logPath does not exist
        if not os.path.isdir(self.logPath): os.mkdir(self.logPath)

        # Check folder for write access
        if os.access(self.logPath, os.W_OK):
            extension = '.' + logFile.split('.')[1]
            log = logFile.split('/')[-1].replace(extension, '.log')
            err = logFile.split('/')[-1].replace(extension, '.errors')
            
            self.logFile = os.path.join(self.logPath, log)
            self.errorLog = os.path.join(self.logPath, err)
            
            if os.path.isfile(self.logFile): os.remove(self.logFile)
        else:
            self.logFile = None

    def log(self, entry:str) -> None:
        """ Log a new entry to the log file
        ### Parameters
        - entry (str) : entry for the log file
        """
        if self.logFile is not None:
            try:
                file = open(self.logFile, "a", encoding="utf8")
                file.write(datetime.now().strftime("%H:%M:%S.%f") + '\t' + entry + '\n')
                file.close()
            except OSError as error:
                return self.errorHandler.handle(error)

    def logError(self, entry:str) -> None:
        """ Log a new error to the log file
        ### Parameters
        - entry (str) : entry for the log file
        """
        if self.errorLog is not None:
            try: 
                file = open(self.errorLog, "a", encoding="utf8")
                file.write(datetime.now().strftime("%H:%M:%S.%f") + '\t' + entry + '\n')
                file.close()
            except OSError as error:
                return self.errorHandler.handle(error)