import os

class Extractor:

    def __init__(self):
        self.counter = 0
        self.items = []

    def outputToConsole(self, outputToConsole:function) -> None:
        """
        Assign the outputToConsole method to this class 

        Args:
            outputToConsole (function) : outputToConsole method
        """        
        self.output = outputToConsole

    def setLogger(self, logger) -> None:
        """
        Assign the logger to this class

        Args:
            logger (Logger): logger instance
        """
        self.logger = logger

    def setErrorHandler(self, errorHandler) -> None:
        """
        Assign the errorHandler to this class

        Args:
            errorHandler (ErrorHandler): ErrorHandler instance
        """        
        self.error = errorHandler

    def setBackupFile(self, backup:str) -> None:
        """
        Assign a backup file to this class

        Args:
            backup (str): name of the backup file
        """        
        self.backupFile = backup

    def setHeader(self, flag:bool) -> None:
        """
        Indicate if file has a header

        Args:
            flag (bool): file has a header
        """        
        self.header = flag

    def setBackup(self, flag:bool) -> None:
        """
        Indicate if the program should attempt to make a backup

        Args:
            flag (bool): program should attempt to make a backup
        """        
        self.backupFlag = flag

    def getItems(self):
        """
        Getter for all items extracted from a file.

        Returns:
            list: list with items extracted from the file (stored on the class)
        """        
        return self.items

    def extract(self, file:str) -> None:
        """
        Extract ID, URN and Giza ID from the file

        Args:
            file (str): File name to extract data from
        """        
        self.counter = 0
        self.items = []
        try: 
            if os.access(file, os.R_OK):
                with open(file, 'r', encoding="utf8") as f:
                    filePath = file.split('/')[-1]
                    # firstLine = f.readline().split('\t')
                    # try:
                    #     if int(firstLine[0]):
                    #         self.header = False
                    # except (OSError, ValueError) as error:
                    #     self.header = True
                    rows = f.read().splitlines()
                    self.logger.log(f'Starting new log for {file}')
                    self.logger.log(f'><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>')
                    self.logger.log(f'Skipping first line (header)') if self.header else self.logger.log(f'Looking for header')
                    self.logger.log(f'><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>')

                    # ITERATE OVER ALL DATA IN FILE
                    for idx, str in enumerate(rows):
                        strings = str.split('\t')
                        if self.header:
                            if idx > 1: ### SKIP HEADER
                                self.pull(strings, idx, filePath)
                        else:
                            self.pull(strings, idx, filePath)
                # self.output(f'EXTRACTOR.EXTRACT: Extracted {self.counter} items from {filePath}')
                
            if self.backupFlag:
                self.backupFile.backup(file)
        except (OSError) as error:
            self.error.handle(error)

    def pull(self, strings:list, idx:int, origin:str) -> None:
        """
        Extracts all relevant parts from a list of strings and adds them to items on the class

        Args:
            strings (list): list of strings
            idx (int): number of item in process
            origin (str): original filepath
        """        
        try:
            ### MAKE SURE WE GET ONE UNIQUE ROW PER ITEM
            if len(strings[10]) is not 0 and strings[13] is not None:
                self.logger.log(f'Extracting data for {strings[0]}@{idx} : {strings[10]}/{strings[13]}')
                self.logger.log(f'\tFound: {strings[4]}, {strings[7]}, {strings[10]}, {strings[11]} and {strings[12]}')
                item = {}
                item['RenditionNumber'] = strings[4]
                item['Type'] = strings[7]
                item['File-ID'] = strings[9]
                item['FileName'] = strings[10]
                form = strings[11].split(' ')
                form.pop()
                f = ''.join(form)
                item['Format'] = f
                item['Size'] = strings[12]
                item['ThumbnailPath'] = strings[10]
                item['Origin'] = origin.split('.')[0]
                self.items.append(item)
                self.counter += 1
        except IndexError as error:
            print(idx, origin, strings, error)