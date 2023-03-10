try:
    from pymssql import OperationalError, Error, DatabaseError, InterfaceError
    import sys, traceback, os
    from socket import gaierror
    from json import JSONDecodeError
except ImportError as error:
    print(error)

res = {}
res['res'] = False

class ErrorHandler:
    """ This class controls the handling of errors.
    ## Methods (3)
    - setLogger : logger (Logger) -> None
    - errorHandler : errorHandler (ErrorHandler) -> None
    - backup : file (str) -> None
    """

    def __init__(self):
        self.path = os.getcwd()

    def setLogger(self, logger) -> None:
        """ Assign the logger to this class
        ### Parameters
        - logger (Logger) : logger instance
        """
        self.logger = logger

    def setVerbosity(self, flag) -> None:
        self.verbose = flag

    def operationalError(self, errorType) -> None:
        if errorType is None: res['msg'] = 'Operational Error'; return res
        if errorType == 'MSSQLDatabaseException':
            try:
                e = self.exception['code'][1]
                e = e.decode('unicode_escape').split('\n')
                e.pop()
                e[0] = e[0].split('.')
                if len(e[0]) > 1:
                    assert e[0][1]
                    e[0][1] = e[0][1].lstrip()
                    assert e[0][2]
                    e[0][2] = e[0][2].split(', ')
                    assert e[0][2][1]
                    e[0][2][1] = e[0][2][1].rstrip(':')
                    e[0][2][1] = e[0][2][1].split()
                    assert e[2]
                    e[2] = e[2].split(', ')
                    if len(e[2] > 1):
                        e[2][1] = e[2][1].rstrip(':')
                        e[2][1] = e[2][1].split()
                    res['msg'] = f'{e[0][0]}. Please check your credentials'
                    if self.verbose:
                        self.logger.logToSystemLog(self.setException(errorType))
                        self.logger.logToSystemLog(f'See also {e[0][2][0]}: {e[0][1]}. The {e[0][2][1][0]} of this error is {e[0][2][1][1]} and thus we cannot continue.') 
                        self.logger.logToSystemLog(f'{e[3]}')
                        self.logger.logToSystemLog(f'{e[1]}. For more information, see {e[2][0]} with {e[2][1][0]} of {e[2][1][1]}')
                else:
                    res['msg'] = e[1]
                    if self.verbose:
                        self.logger.logToSystemLog(self.setException(errorType))
                        self.logger.logToSystemLog(f'{e[1]}')
                        self.logger.logToSystemLog(f'For further information, see {e[2]}')
                return res
            except IndexError as error:
                self.handle(error)

    def setException(self, errorType:str) -> str:
        return f'{self.exception["file"]} raised an {errorType} at line {self.exception["line"]} in {self.exception["file"]} with the following reason:'

    def attributeError(self) -> None:
        if self.verbose:
            self.logger.logToSystemLog(f'Generic AttributeError raised on line {self.exception["line"]} in {self.exception["file"]}: {self.exception["code"]}')
        pass

    def databaseError(self) -> None:
        """ Not implemented
        ### Parameters
        - errorType (errorType) : errorType
        """
        print('Database Error!')

    def genericError(self) -> None:
        """ Not implemented
        ### Parameters
        - errorType (errorType) : errorType
        """
        print('Generic Error!')

    def assertionError(self) -> None:
        """ Not implemented
        ### Parameters
        - errorType (errorType) : errorType
        """
        pass

    def interfaceError(self, errorType) -> dict:
        """ Process interface (connection) errors
        ### Parameters
        - errorType (errorType) : errorType
        """
        res['msg'] = f'{self.exception["code"]}. Check your internet connection.'
        if self.verbose:
            self.logger.logToSystemLog(self.setException(errorType))
            self.logger.logToSystemLog(f'{self.exception["code"]}')
        return res

    def indexError(self) -> None:
        """ Not implemented
        """
        print('Index Error!')
        pass

    def gaiError(self, errorType) -> dict:
        """ Not implemented
        ### Parameters
        - errorType (errorType) : errorType
        """
        res['msg'] = f'Something is wrong with your connection...'
        if self.verbose:
            self.logger.logToSystemLog(self.setException(errorType))
            self.logger.logToSystemLog(f'{self.exception["code"]}')
        return res

    def permissionError(self) -> dict:
        res = {}
        res['res'] = False
        res['msg'] = f'{self.exception["code"][1]}. Check you have only one instance of this program running and nothing else is accessing the file or try and removing it manually.'
        if self.verbose:
            self.logger.logToSystemLog(self.setException(self.exception["type"]))
            self.logger.logToSystemLog(f'Permission Error: {self.exception["file"]} cannot access a required file; it is accessed by another process. Check you have only one instance of this program running and nothing else is accessing the file.')
        return res

    # def connectionError(self):
        # return f'DB.CHECKACCESS: Error connecting to {exthost} aka {self.host} on port {self.port}. Are you in the right domain/is your VPN active?'

    def JSONDecodeError(self):
        if self.verbose:
            self.logger.logToSystemLog(f'Could not decode local JSON table. File has been removed.')
        return self.res()

    def res(self, err:str=None) -> dict:
        res['msg'] = err
        return res

    def handle(self, e, errorType=None):
        """ Handle the error
        ### Parameters
        - e (ErrorType)
        - errorType (ErrorType) : errorType
        """
                
        self.exception = {}
        try:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # assert exc_value.args[0]
            self.exception['type'] = exc_type
            # assert exc_value.args[0][0]
            self.exception['code'] = exc_value.args if len(exc_value.args) else 'None'
            self.exception['file'] = exc_traceback.tb_frame.f_code.co_filename
            self.exception['line'] = exc_traceback.tb_lineno
        except IndexError as error:
            self.handle(error)
        
        if isinstance(e, InterfaceError): return self.interfaceError(errorType)
        if isinstance(e, OperationalError): return self.operationalError(errorType)
        if isinstance(e, AttributeError): return self.attributeError()
        if isinstance(e, DatabaseError): return self.databaseError(errorType)
        if isinstance(e, Error): return self.genericError()
        if isinstance(e, AssertionError): return self.assertionError()
        if isinstance(e, IndexError): return self.indexError()
        if isinstance(e, gaierror): return self.gaiError('gaierror')
        if isinstance(e, PermissionError): return self.permissionError()
        if isinstance(e, JSONDecodeError): return self.JSONDecodeError()
        if 'ConnectionError' in e: return self.connectionError()