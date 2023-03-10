import os
from tinydb import TinyDB, Query

flags, db = {}, {}

class Settings:
    """ This class controls writing and reading settings to memory with the use of TinyDB.
    ## Methods (3)
    - setLogger : logger (Logger) -> None
    - loadSettings : None -> None
    - saveSettings : newFlags (list), newDB (list) -> None
    """

    def __init__(self):
        self.path = os.getcwd()
        self.config = os.path.join(self.path, 'offloader.config')

    def setLogger(self, logger) -> None:
        """ Assign the logger to this class
        ### Parameters
        - logger (Logger) : logger instance
        """
        self.logger = logger

    def loadSettings(self) -> None:
        """ Read settings from the TinyDB database 
        ### Parameters
        None
        ### Returns
        - result (dict) : dictionary with three properties:
            - msg : result of read action
            - db : keys read from database
            - flags : values read from database
            
        """
        try:
            os.access(self.config, os.R_OK) # CHECK WRITE ACCESS TO SETTINGS FILE
            try:
                settings = TinyDB(self.config, default_table='Settings', sort_keys=True, indent=4, separators=(',', ': '))
                setting = Query()
                
                newFlags = settings.search(setting.type == 'flag')
                newDB = settings.search(setting.type == 'db')
                flags, db = {}, {}
                for flag in newFlags: 
                    flags[flag['key']] = flag['value']
                for item in newDB: db[item['key']] = item['value']
                result = {}
                result['msg'] = f'SETTINGS.LOADSETTINGS: Settings successfully loaded'
                result['db'] = db
                result['flags'] = flags
                return result
            except IndexError as error:
                result = {}
                result['msg'] = error
                result['db'] = { 'host' : '', 'name' : '', 'user' : '', 'scratchTable' : ''}
                result['flags'] = { 'lFlag' : False, 'eFlag' : False, 'bFlag' : False, 'tFlag' : False, 'hFlag' : False, 'mFlag' : False }
                return result
            # else:
            #     result = {}
            #     result['msg'] = f'SETTINGS.LOADSETTINGS: Unable to load settings'
            #     result['flags'] = { 'lFlag' : False, 'eFlag' : False, 'bFlag' : False, 'tFlag' : False, 'hFlag' : False, 'mFlag' : False }
            #     result['db'] = { 'host' : '', 'name' : '', 'user' : '', 'scratchTable' : ''}     
            #     return result
        except OSError as error:
            result['msg'] = error

    def saveSettings(self, newFlags:list, newDB:list) -> None:
        """ Write settings to the TinyDB database 
        ### Parameters
        - newFlags (list) : flags to write to the database
        - newDB (list) : keys to write to the database
        """
        try:
            os.access(self.config, os.W_OK) # CHECK WRITE ACCESS TO SETTINGS FILE
            settingsTable = TinyDB(self.config, sort_keys=True, indent=4, separators=(',', ': '))
            settingsTable.truncate()
            settingsTable.insert_multiple({ 'type' : 'db', 'key' : s, 'value' : newDB[s] } for s in newDB)
            settingsTable.insert_multiple({ 'type' : 'flag', 'key' : f, 'value' : newFlags[f] } for f in newFlags)
            return f'SETTINGS.SAVESETTINGS: Settings updated'
        except OSError as error:
            if flags['eFlag'] == 1: self.logger.logError(error)