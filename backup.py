import os

class Backup:
    """ This class controls backup of files.
    ## Methods (3)
    - setLogger : logger (Logger) -> None
    - errorHandler : errorHandler (ErrorHandler) -> None
    - backup : file (str) -> None
    """

    def __init__(self):
        self.processedPath = os.path.join(os.getcwd(), 'processed')

    def setLogger(self, logger):
        """ Assign the logger to this class
        ### Parameters
        - logger (Logger) : logger instance
        """
        self.logger = logger

    def errorHandler(self, errorHandler):
        """ Assign the errorHandler to this class 
        ### Parameters
        - errorHandler (ErrorHandler) : ErrorHandler instance
        """
        self.error = errorHandler

    def backup(self, file:str) -> None:
        """ Moves a copy of the given file to a backup location
        ### Parameters
        - file (str) : file name to be copied for backup
        """
        
        if not os.path.isdir(self.processedPath): os.mkdir(self.processedPath)
        
        try:
            f = file.split('/')[-1]
            # relPath = os.path.relpath(file)
            originalFilePath = os.path.join(os.getcwd(), os.path.relpath(file))
            newFilePath = os.path.join(self.processedPath, f)

            # Check write access to the new folder
            if os.access(self.processedPath, os.W_OK):
                if os.path.isfile(file):
                    try:
                        if os.path.isfile(newFilePath): os.remove(newFilePath)
                        os.rename(originalFilePath, newFilePath)
                        self.logger.log(f'BACKUP.BACKUP: Moved {file} to {newFilePath}')
                    except (OSError) as error:
                        # print(error)
                        self.logger.logError(error)
        except OSError as error:
            self.error.handle(error)