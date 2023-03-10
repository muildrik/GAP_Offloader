try:
    import os, json, asyncio, multiprocessing, sys, random, time
    
    from multiprocessing.pool import ThreadPool
    from multiprocessing import Queue, Process
    from multiprocessing.queues import Empty
    from thread import ThreadedTask
    from threading import Thread
    
    from tkinter import simpledialog, filedialog, messagebox, END, Tk, StringVar, BooleanVar, Label, LabelFrame, Checkbutton, Button, Entry, NORMAL, DISABLED, Scrollbar, Text, HORIZONTAL, BOTH, X, Y, LEFT, RIGHT, TOP, Frame, ttk, DoubleVar
    from tkinter.ttk import Progressbar
    
    from datetime import datetime
    
    # PROGRAM DEPENDENCIES
    from db import DB
    from extractor import Extractor
    from settings import Settings
    from errorHandler import ErrorHandler
    from backup import Backup
    from logger import Logger

except ImportError as error:
    print(error)

def updateItem(Q:Queue, PP:Queue, items:list, mmids:list, mtids:list, mmidsonly:list, mtidsonly:list) -> None:
    """
    Updates a single item in items based on mmids and mtids

    Args:
        Q (Queue): Queue to be updated
        PP (Queue): Temporary queue
        items (list): all items to be processed
        mmids (list): mmids and other data
        mtids (list): mtids and other data
        mmidsonly (list): only mmids
        mtidsonly (list): only mtids
    """    
    problemRecs = []
    i = 0
    while i < len(items):
        try:
            item = items[i]
            PP.put([item["RenditionNumber"], item["Origin"]])
            if item["RenditionNumber"] in mmidsonly:
                idx = mmidsonly.index(item["RenditionNumber"])
                item.update(mmids[idx])
                if item["Format"] in mtidsonly:
                    idx = mtidsonly.index(item["Format"])
                    item.update(mtids[idx])
                else:
                    problemRecs.append(items.pop(i))
            else:
                problemRecs.append(items.pop(i))
            i += 1
        except:
            problemRecs.append(items.pop(i))
    Q.put([items, problemRecs])

def updateFileID(Q:Queue, PP:Queue, items:list, iids:list, iidsonly:list) -> None:
    """
    Update the fileID for each item

    Args:
        Q (Queue): Queue to be updated
        PP (Queue): Temporary queue
        items (list): all items to be processed
        iids (list): iids and other data
        iidsonly (list): iids only
    """    
    problemRecs = []
    i = 0
    while i < len(items):
        try:
            item = items[i]
            PP.put([item["RenditionNumber"], item["Origin"]])
            if item["RenditionID"] in iidsonly:
                idx = iidsonly.index(item["RenditionID"])
                item.update(iids[idx])
            else:
                problemRecs.append(items.pop(i))
        except:
            problemRecs.append(items.pop(i))
        i += 1
    Q.put([items, problemRecs])

def verifyItem(Q:Queue, PP:Queue, items:list, mmidsonly:list) -> None:
    """
    Join up each item and double-check the rendition numbers and file ids match up

    Args:
        Q (Queue): Queue to be updated
        PP (Queue): Temporary queue
        items (list): all items to be processed
        mmidsonly (list): mmidsonly
    """
    problemRecs = []
    i = 0
    while i < len(items):
        try:
            item = items[i]
            PP.put([item["RenditionNumber"], item["Origin"]])
            mmidx = mmidsonly["RenditionNumbers"].index(item["RenditionNumber"])
            fidx = mmidsonly["PrimaryFileIDs"][mmidx]
            if fidx != item["FileID"]:
                problemRecs.append(items.pop(i))
        except OSError as error:
            print(error)
            problemRecs.append(items.pop(i))
        i += 1
    Q.put([items, problemRecs])

def calcProcessTime(starttime:int, cur_iter:int, max_iter:int) -> tuple:
    """
    Not implemented. Purpose is to calculate overall processing time.

    Args:
        starttime (int): the time of starting the operation
        cur_iter (int): the current interation
        max_iter (int): the maximum number of iterations

    Returns:
        tuple: three values: how much time elapsed, how much left and estimated time to completion.
    """
    telapsed = time.time() - starttime
    testimated = (telapsed/cur_iter)*(max_iter)
    elapsed = datetime.fromtimestamp(telapsed).strftime("%M:%S")

    lefttime = testimated-telapsed  # in seconds
    left = datetime.fromtimestamp(lefttime).strftime("%M:%S")

    finishtime = starttime + testimated
    finishtime = datetime.fromtimestamp(finishtime).strftime("%H:%M:%S")  # in time

    return (elapsed, left, finishtime)

class GUIConverter(Tk):
    """
    GUI Class with basic configuration and Tkinter elements
    """    

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        icon = os.path.join(os.getcwd(), 'Pyramids.ico')

        self.wm_state('zoomed')

        """ GUI CONFIGURATION """
        # self = Tk()
        self.config(padx=10, pady=10)
        self.title('Giza Archives DRS Report Converter')
        self.iconbitmap(icon)
        self.var = 0
        self.flags, self.db = {}, {}
        self.mmids = Queue()
        self.mtids = Queue()
        self.iids = Queue()
        self.MMIDSResult = False
        self.MTIDSResult = False
        self.IIDSResult = False
        self.funcs = {}
        # self.connectionVerified = False
        self.verbosity = False
        self.busy = False
        self.items = []
        self.cpuCount = multiprocessing.cpu_count()
        self.newRecords = []
        self.pBarSteps = 100
        self.Q1 = Queue()
        self.Q2 = Queue()
        self.Q1Result = False
        self.Q2Result = False
        self.Q1Func = None
        self.Q2Func = None
        self.problemRecs = []
        self.Queues = []
        self.Qs = []
        # self.Ps = []

        """ DEFINE KEY VARIABLES """
        self.db['host'] = StringVar()
        # self.db['port'] = IntVar()
        self.db['scratchTable'] = StringVar()
        self.db['name'] = StringVar()
        self.db['user'] = StringVar()
        self.flags['lFlag'] = BooleanVar()
        self.flags['eFlag'] = BooleanVar()
        self.flags['bFlag'] = BooleanVar()
        self.flags['tFlag'] = BooleanVar()
        self.flags['hFlag'] = BooleanVar()
        self.flags['mFlag'] = BooleanVar()

        """ INITIALIZE DEPENDENCIES """
        self.settings = Settings()
        self.logger = Logger()
        self.errorHandler = ErrorHandler()
        self.extractor = Extractor()
        self.backup = Backup()
        
        self.DB = DB()
        self.DB.askQuestion(self.askQuestion)
        self.DB.notifyUser(self.notify)
        self.DB.verification(self.verifyConnection)
        # self.DB.beginUpdate(self.batchUpdate)

        """ CONFIGURE DEPENDENCIES """
        self.preferences = self.settings.loadSettings()
        
        self.logger.setErrorHandler(self.errorHandler)
        self.logger.outputToConsole(self.outputToConsole)
        
        self.errorHandler.setLogger(self.logger)
        
        self.extractor.setLogger(self.logger)
        self.extractor.setErrorHandler(self.errorHandler)
        self.extractor.outputToConsole(self.outputToConsole)

        self.backup.setLogger(self.logger)
        self.backup.errorHandler(self.errorHandler)
        
        self.logger.setLogStartTime(datetime.now())      # START LOGGING AT BEGIN OF PROGRAM
        self.logger.startSystemLog('systemLog.log')     # LOG SYSTEM MESSAGES
        # self.errorHandler = ErrorHandler(self.flags['eFlag'].get())
        # self.errorHandler.setLogger(self.logger)

        """ SET INITIAL VALUES LOADED FROM SETTINGS """
        self.db['host'].set(self.preferences['db']['host'])                     # SET HOST NAME
        # self.db['port'].set(self.preferences['db']['port'])                     # SET SERVER PORT
        self.db['name'].set(self.preferences['db']['name'])                     # SET DATABASE NAME
        self.db['user'].set(self.preferences['db']['user'])                     # SET USERNAME
        self.db['scratchTable'].set(self.preferences['db']['scratchTable'])     # SET TEMP DB

        self.flags['lFlag'].set(self.preferences['flags']['lFlag'])    # LOG PROCESS TO FILE
        self.flags['eFlag'].set(self.preferences['flags']['eFlag'])    # VERBOSE ERROR MESSAGES
        self.flags['bFlag'].set(self.preferences['flags']['bFlag'])    # BACKUP FILES TO A PROCESSED FOLDER
        self.flags['tFlag'].set(self.preferences['flags']['tFlag'])    # PUSH UPDATES TO TMS
        self.flags['hFlag'].set(self.preferences['flags']['hFlag'])    # PROCESS FILE WITH HEADER ROW
        self.flags['mFlag'].set(self.preferences['flags']['mFlag'])    # SUPPRESS ALL MESSAGES

        self.extractor.setHeader(self.flags['hFlag'].get())
        self.extractor.setBackup(self.flags['bFlag'].get())
        self.extractor.setBackupFile(self.backup)
        self.errorHandler.setVerbosity(self.flags['eFlag'].get())
        self.DB.setLogger(self.logger)
        self.DB.setErrorHandler(self.errorHandler)
        self.DB.setHost(self.db['host'].get())
        self.DB.setName(self.db['name'].get())
        self.DB.setUser(self.db['user'].get())
        self.DB.setScratchTable(self.db['scratchTable'].get())
        self.DB.setDBUpdate(self.flags['tFlag'].get())
        self.DB.outputToConsole(self.updatePBar, self.outputToConsole)

        """ BIND SHORTCUT TO EXIT BUTTON """
        self.focus_set()
        self.funcs['Control-p'] = lambda e: self.processFiles()
        self.funcs['Control-o'] = lambda e: self.loadFileNames()
        self.funcs['Control-s'] = lambda e: self.save()
        self.funcs['Control-x'] = lambda e: self.exit()
        self.funcs['Control-c'] = lambda e: self.checkConnection()
        # self.funcs['Control-P'] = lambda e: self.DB.delTempTable()
        self.funcs['Control-l'] = lambda e: self.toggle('lFlag')
        self.funcs['Control-m'] = lambda e: self.toggle('mFlag')
        self.funcs['Control-g'] = lambda e: self.toggle('hFlag')
        self.funcs['Control-u'] = lambda e: self.toggle('tFlag')
        self.funcs['Control-v'] = lambda e: self.toggle('eFlag')
        self.funcs['Alt-c'] = lambda e: self.clearConsole()
        self.toggleBindings('Control', ['o', 's', 'x', 'l', 'c'])
        self.toggleBindings('Alt', ['c'])

        """ ADD TRACERS TO THE VARIABLES WHEN VALUES GET UPDATED """
        # self.db['port'].trace_add("write", lambda name, index, mode, dbp=self.db['port']: self.checkVal(self.TMSDBPNum, self.db['port']))
        # self.db['name'].trace_add("write", lambda name, index, mode, dbn=self.db['name']: self.checkVal(self.TMSDBNTxt, self.db['name']))
        # self.db['user'].trace_add("write", lambda name, index, mode, dbu=self.db['user']: self.checkVal(self.TMSDBUTxt, self.db['user']))
        # self.db['host'].trace_add("write", lambda name, index, mode, dbh=self.db['host']: self.checkVal(self.TMSDBHTxt, self.db['host']))

        """ FRAME FOR ESSENTIAL CONTROLS """
        self.Frame = Frame(self)
        self.Frame.pack(fill=BOTH, expand=1)

        """ LABELFRAME FOR FILE SELECTION AND PROCESSING """
        self.FileLF = LabelFrame(self.Frame, text="1. SELECT DRS FILES TO PROCESS", padx=5, pady=5)
        self.FileLF.pack(fill=X, expand=1, anchor='n')
        
        # BUTTONS
        self.FileSelectDRSReports = Button(self.FileLF, underline=0, text="Open DRS report(s)", command=self.loadFileNames)
        self.FileSelectDRSReports.pack(side=LEFT, pady=3, padx=3)

        # LABELS
        self.FileSelectedReports = Label(self.FileLF, text="")
        self.FileSelectedReports.pack(side=LEFT, pady=3, padx=3)

        """ LABELFRAME FOR ADDITIONAL SETTINGS """
        self.SettingsLF = LabelFrame(self.Frame, text="2. SET ADDITIONAL OPTIONS", padx=5, pady=5)
        self.SettingsLF.pack(fill=X, anchor='n')

        # CHECKBUTTONS
        self.SettingsLog = Checkbutton(self.SettingsLF, text="Log process to file", underline=0, variable=self.flags['lFlag'], command=self.writeToLog)
        self.SettingsLog.pack(anchor='w')

        self.SettingsMove = Checkbutton(self.SettingsLF, text="Move processed file", underline=0, variable=self.flags['bFlag'], command=self.backupFiles)
        self.SettingsMove.pack(anchor='w')

        self.SettingsHeader = Checkbutton(self.SettingsLF, text="Ignore header row", underline=1, variable=self.flags['hFlag'], command=self.headerRow)
        self.SettingsHeader.pack(anchor='w')

        self.SettingsPushToTMS = Checkbutton(self.SettingsLF, text="Push updates to TMS", underline=1, variable=self.flags['tFlag'], command=self.pushToTMS)
        self.SettingsPushToTMS.pack(anchor='w')

        self.SettingsLog = Checkbutton(self.SettingsLF, text="Verbose error logging", underline=0, variable=self.flags['eFlag'], command=self.verboseErrors)
        self.SettingsLog.pack(anchor='w')

        """ LABELFRAME FOR TMS SETTINGS """
        self.TMSSettingsLF = LabelFrame(self.Frame, text="3. ADJUST CONNECTION SETTINGS IF NECESSARY", padx=5, pady=5)
        self.TMSSettingsLF.pack(fill=BOTH, side=TOP)
        
        # HOST NAME
        self.TMSHostFrame = Frame(self.TMSSettingsLF)
        self.TMSHostFrame.pack(anchor='w')
        
        self.TMSDBHLbl = Label(self.TMSHostFrame, text="Host:", takefocus=False)
        self.TMSDBHLbl.pack(side=LEFT, anchor='w')

        self.TMSDBHTxt = Entry(self.TMSHostFrame, textvariable=self.db['host'], width=50)
        self.TMSDBHTxt.pack(side=LEFT)

        # ### PORT NUMBER
        # self.TMSDBPLbl = Label(self.TMSSettingsLF, text="Port number:", takefocus=False)
        # self.TMSDBPLbl.grid(row=0, column=2, sticky='w', padx=5, pady=2)
        # self.TMSDBPNum = Entry(self.TMSSettingsLF, textvariable=self.db['port'], name="port", width=5, justify='center')
        # self.TMSDBPNum.grid(row=0, column=3, sticky="w", pady=3)

        # DATABASE NAME
        self.TMSNameFrame = Frame(self.TMSSettingsLF)
        self.TMSNameFrame.pack(anchor='w')

        self.TMSDBNLbl = Label(self.TMSNameFrame, text="Database name:", takefocus=False)
        self.TMSDBNLbl.pack(side=LEFT, anchor='w')

        self.TMSDBNTxt = Entry(self.TMSNameFrame, textvariable=self.db['name'])
        self.TMSDBNTxt.pack(side=LEFT)

        self.TMSUserFrame = Frame(self.TMSSettingsLF)
        self.TMSUserFrame.pack(anchor='w')
        
        # USERNAME
        self.TMSDBULbl = Label(self.TMSUserFrame, text="Username:", takefocus=False)
        self.TMSDBULbl.pack(side=LEFT)

        self.TMSDBUTxt = Entry(self.TMSUserFrame, textvariable=self.db['user'])
        self.TMSDBUTxt.pack(side=LEFT)

        self.TMSScratchFrame = Frame(self.TMSSettingsLF)
        self.TMSScratchFrame.pack(anchor='w')

        self.TMSDBTTLbl = Label(self.TMSScratchFrame, text="Scratch table:", takefocus=False)
        self.TMSDBTTLbl.pack(side=LEFT, fill=BOTH)

        self.TMSDBTTTxt = Entry(self.TMSScratchFrame, textvariable=self.db['scratchTable'])
        self.TMSDBTTTxt.pack(side=LEFT)

        # TEST CONNECTION BUTTON
        self.TMSDBTBtn = Button(self.TMSSettingsLF, text="Check connection", underline=0, command=self.checkConnection, state=DISABLED)
        self.TMSDBTBtn.pack(side=LEFT)

        # PROCESS FILES
        self.FileProcessFilesBtn = Button(self.TMSSettingsLF, text="Process files", underline=0, command=self.processFiles, state=DISABLED)
        self.FileProcessFilesBtn.pack(side=LEFT)

        # self.TMSDBPBtn = Button(self.TMSSettingsLF, text="Purge temp tables", underline=0, command=self.DB.delTempTable, state=DISABLED)
        # self.TMSDBPBtn.pack(side=LEFT)

        """ LABELFRAME FOR OUTPUT """
        self.OutputLF = LabelFrame(self, text="Output", bd=0)
        self.OutputLF.pack(fill=X)
        
        ### OUTPUT WINDOW
        self.OutputTextBox = Text(self.OutputLF)
        self.OutputTextBox.pack(side=LEFT, fill=BOTH, expand=1)

        self.OutputScrollBar = Scrollbar(self.OutputLF)
        self.OutputScrollBar.pack(expand=1, fill=BOTH)

        self.OutputScrollBar.config(command=self.OutputTextBox.yview)
        self.OutputTextBox.config(yscrollcommand=self.OutputScrollBar.set)

        """ LABELFRAME FOR FINAL BUTTONS """
        self.FinalLF = LabelFrame(self.Frame, bd=0, padx=5, pady=5)
        self.FinalLF.pack(fill=BOTH)

        """ PROCESS BUTTON """
        # CLEAR CONSOLE
        self.ClearConsoleBtn = Button(self.FinalLF, text="Clear console", underline=0, command=self.clearConsole)
        self.ClearConsoleBtn.pack(side=LEFT)

        """ PROGRESS BAR """
        self.style = ttk.Style(self)
        self.style.layout('text.Horizonal.TProgressbar', [('Horizontal.Progressbar.trough', {'children': [('Horizontal.Progressbar.self.pbar', {'side': 'left', 'sticky': 'ns'})], 'sticky': 'nswe'}), ('Horizontal.Progressbar.label', {'sticky': ''})])
        self.style.configure('text.Horizontal.TProgressbar', text='0 %')
        self.progressVar = DoubleVar(self.FinalLF)
        self.progressBar = Progressbar(self.FinalLF, orient=HORIZONTAL, length=500, mode="determinate", takefocus=True, maximum=100, style='text.Horizontal.TProgressbar', variable=self.progressVar)
        self.progressBar.pack(side=LEFT, padx=5, fill=BOTH, expand=1)

        # EXIT BUTTON
        self.ExitDRSConverter = Button(self.FinalLF, text="Exit", underline=1, command=self.exit)
        self.ExitDRSConverter.pack(side=RIGHT)

        # SAVE SETTINGS
        self.TMSSaveSettings = Button(self.FinalLF, text="Save settings", underline=0, command=self.save)
        self.TMSSaveSettings.pack(side=RIGHT)

        self.Counter = Label(self.TMSSettingsLF)
        self.Counter.pack(side=RIGHT)
        
        # self.TMSDBPNum.bind("<FocusIn>", lambda TMSDBPNum : self.focus(self.TMSDBPNum, self.db['port']))
        # self.TMSDBPNum.bind("<FocusOut>", lambda TMSDBPNum : self.unfocus(self.TMSDBPNum, self.db['port']))
        # self.TMSDBNTxt.bind("<FocusIn>", lambda TMSDBNTxt : self.focus(self.TMSDBNTxt, self.db['name']))
        # self.TMSDBNTxt.bind("<FocusOut>", lambda TMSDBNTxt : self.unfocus(self.TMSDBNTxt, self.db['name']))
        # self.TMSDBUTxt.bind("<FocusIn>", lambda TMSDBUTxt : self.focus(self.TMSDBUTxt, self.db['user']))
        # self.TMSDBUTxt.bind("<FocusOut>", lambda TMSDBUTxt : self.unfocus(self.TMSDBUTxt, self.db['user']))
        # self.TMSDBHTxt.bind("<FocusIn>", lambda TMSDBHTxt : self.focus(self.TMSDBHTxt, self.db['host']))
        # self.TMSDBHTxt.bind("<FocusOut>", lambda TMSDBHTxt : self.unfocus(self.TMSDBHTxt, self.db['host']))

    def toggle(self, flag:str) -> None:
        """
        Controls updating settings by the user

        Args:
            flag (str): name of the flag to update
        """        
        self.flags[flag].set(False) if self.flags[flag].get() else self.flags[flag].set(True)
        if 'lFlag' in flag: self.writeToLog()
        if 'mFlag' in flag: self.backupFiles()
        if 'hFlag' in flag: self.headerRow()
        if 'tFlag' in flag: self.pushToTMS()
        if 'eFlag' in flag: self.verboseErrors()

    def updatePBar(self, steps:int=None) -> None:
        """
        Update the progress bar element

        Args:
            steps (int, optional): how many steps out of 100 to update the progress bar. Defaults to None.
        """        
        if steps is None:
            self.progressBar.step()
            self.style.configure('text.Horizontal.TProgressbar', text='{:g} %'.format(self.progressVar.get() * (100/self.pBarSteps)))
            self.update()
        elif steps == 0:
            self.progressBar['value'] = 0
        else:
            self.pBarSteps = steps
            self.progressBar.config(maximum=steps)

    def askQuestion(self, title:str, msg:str) -> bool:
        """
        _summary_

        Args:
            title (str): title of message
            msg (str): message to ask user

        Returns:
            bool: result of user's answer to question
        """          
        return messagebox.askyesno(title, msg)

    def notify(self, msg:dict) -> str:
        """
        _summary_

        Args:
            msg (dict): dictionary with res and msg properties

        Returns:
            str: not used
        """        
        if msg['res'] is False:
            messagebox.showerror('Critical error!', msg['msg'])
            self.outputToConsole('>>>>>> PROCESS INTERRUPTED <<<<<<', False)
        elif msg['res'] is True:
            messagebox.showinfo('Info', msg['msg'])
        elif msg['res'] is None:
            return messagebox.showwarning('Warning', msg['msg'])

    def toggleControls(self, el, state):
        for child in el.winfo_children():
            if 'button' in child._name or 'entry' in child._name:
                child.config(state=state)
            else:
                self.toggleControls(child, state)

    def toggleBindings(self, k:str='Control', keys:list=None, state:bool=True) -> None:
        """
        Assign shortcuts to the Tkinter interface

        Args:
            k (str, optional): Shift, alt and control keys. Defaults to 'Control'.
            keys (list, optional): List of letters. Defaults to None.
            state (bool, optional): Bind or unbind key. Defaults to True.
        """        
        for key in keys:
            kkey = f'{k}-{key}'
            if state:
                self.bind(f'<{kkey}>', self.funcs[kkey])
            else:
                self.unbind(f'<{kkey}>')

    def loadFileNames(self) -> None:
        """
        Load file for processing
        """        
        # self.input = filedialog.askopenfilenames(initialdir=os.getcwd(), title="Select report file", filetypes = (("DRS report","*.txt"),))
        self.input = [os.path.join(os.getcwd(), 'drs_report3.txt')] #### DELETE AFTER TESTING
        self.TMSDBTBtn.config(state=('normal' if self.input else 'disabled'))
        self.FileProcessFilesBtn.config(state='normal') #### DELETE AFTER TESTING
        self.toggleBindings('Control', ['c'])
        [self.outputToConsole(f'OFFLOADER: Selected "{x}" for processing') for x in self.input]
        if self.input:
            self.FileSelectedReports.config(text = (f'{len(self.input)} {"file" if len(self.input) == 1 else "files"} selected'))
            self.logger.logToSystemLog(f'OFFLOADER: {len(self.input)} {"file" if len(self.input) == 1 else "files"} selected')

    def checkConnection(self) -> None:
        """
        Set controls, check TinyDB is good and the connection to the TMS instance
        """        
        self.toggleControls(self.Frame, DISABLED)
        self.toggleBindings('Control', ['o', 'p', 's', 'l', 'm', 'g', 'u', 'v', 'c'], False)
        self.toggleBindings('Alt', ['c'], False)
        # password = simpledialog.askstring("Password", "Enter password:", show='*')
        password = 'set to some password' #### DELETE AFTER TESTING

        self.DB.checkTinyDB()

        self.outputToConsole('OFFLOADER: Verifying connection settings')
        self.updatePBar(5)

        self.makeQueue(self.getRandInt(), self.DB.checkAccess, (password,))

    def verifyConnection(self, verification:bool) -> None:
        """
        Update user when connection has been checked

        Args:
            verification (bool): result of verification procedure
        """        
        if verification is False:
            self.outputToConsole('>>>>>> THERE WAS A PROBLEM SETTING UP ESSENTIAL PARAMETERS <<<<<<', False)
        else:
            self.connection = verification
            self.FileProcessFilesBtn.config(state='normal')
            self.toggleControls(self.Frame, 'normal')
            self.toggleBindings('Control', ['c'], False)
            self.toggleBindings('Control', ['p', 'l', 'm', 'g', 'u', 'v'])
            self.outputToConsole('>>>>>> READY TO PROCESS SELECTED FILES <<<<<<', True)

    def save(self) -> None:
        """
        Save settings to the TinyDB
        """        
        newFlags = {k : v.get() for (k, v) in self.flags.items()}
        newDB = {k : v.get() for (k, v) in self.db.items()}
        self.outputToConsole(self.settings.saveSettings(newFlags, newDB))

    def writeToLog(self) -> None:
        """
        Write log files for each file
        """        
        self.logger.shouldWeLog(self.flags['lFlag'].get())
        self.logger.logToSystemLog('OFFLOADER: Elected to write logs for each file') if self.flags['lFlag'].get() else self.logger.logToSystemLog('OFFLOADER: Not elected to write logs for each file')

    def backupFiles(self) -> None:
        """
        Back up files for each file
        """        
        self.extractor.setBackup(self.flags['bFlag'].get())
        self.logger.logToSystemLog(f'OFFLOADER: Processed files will be stored in {os.path.join(os.getcwd(), "processed")}') if self.flags['bFlag'].get() else self.logger.logToSystemLog('OFFLOADER: Files will not be moved after processing')

    def headerRow(self) -> None:
        """
        Assume files have headers
        """        
        self.extractor.setHeader(self.flags['hFlag'].get())
        self.logger.logToSystemLog(f'OFFLOADER: First row indicated to contain header data') if self.flags['hFlag'].get() else self.logger.logToSystemLog(f'OFFLOADER: First row indicated to not contain header data')

    def verboseErrors(self) -> None:
        """
        Verbose error message logging
        """        
        self.verbosity = self.flags['eFlag'].get()
        self.errorHandler.setVerbosity(self.flags['eFlag'].get())
        self.logger.logToSystemLog('OFFLOADER: Verbose logging enabled') if self.flags['eFlag'].get() else self.logger.logToSystemLog('OFFLOADER: Verbose logging disabled')
    
    def pushToTMS(self) -> None:
        """
        Should we try to push data to the TMS tables?
        """        
        if self.flags['tFlag'].get():
            self.toggleControls(self.TMSSettingsLF, NORMAL)
            self.FileProcessFilesBtn.config(state=DISABLED)
            if self.db['name'].get() and self.db['host'].get() and self.db['user'].get():
                self.DB.setDBUpdate(self.flags['tFlag'].get())
                self.outputToConsole(f'Pushing to {self.db["name"].get()} @ {self.db["host"].get()} as user {self.db["user"].get()}')
            else:
                self.outputToConsole(f'OFFLOADER: Please provide all connection details')
                self.flags['tFlag'].set(False)
        else:
            self.toggleControls(self.TMSSettingsLF, DISABLED)

    def clearConsole(self) -> None:
        """
        Clear the output console
        """        
        self.OutputTextBox.config(state=NORMAL)
        self.OutputTextBox.delete(1.0, END)
        self.OutputTextBox.config(state=DISABLED)

    def processFiles(self) -> None:
        """
        Begin processing of files
        """        
        self.outputToConsole(f'OFFLOADER: Beginning batch update process; please do not interrupt!')
        self.toggleControls(self.Frame, DISABLED)
        self.toggleBindings('Control', ['o', 'p', 's', 'l', 'm', 'g', 'u', 'v'], False)
        self.toggleBindings('Alt', ['c'], False)

        for i in self.input:
            if self.flags['lFlag'].get():
                self.logger.newLog(i)
            self.extractor.extract(i)
            self.items.append(self.extractor.getItems())

        if self.connection:
            self.makeQueue(self.getRandInt(), self.DB.getMTIDs)
            # self.setQ1(self.DB.getMTIDs)
        else:
            self.outputToConsole('OFFLOADER: Connection not verified; exiting update process', False)

        # ENABLE ALL SHORT CUT BINDINGS
        self.toggleBindings('Control', ['o', 'p', 's', 'l', 'm', 'g', 'u', 'v'])

    def updateItems(self) -> None:
        """
        Batch update self.items in memory with multiprocessing to speed up overall progress. Data is split and chunks assigned to different processes. Processes are queued and processed until all have been completed.
        """        
        self.outputToConsole('>>>>>> BATCH UPDATING RECORDS <<<<<<', True)
        items = [item for sublist in self.items for item in sublist]
        
        if len(items) > 100: self.outputToConsole('OFFLOADER: This might take a while...')

        # begin = time.time()

        allMMIDs = self.DB.getAllMMIDS()
        allMTIDs = self.DB.getAllMTIDS()
        MMIDsOnly = [id["RenditionNumber"] for id in allMMIDs]
        MTIDsOnly = [id["Format"] for id in allMTIDs]

        loadLength = round(len(items) / self.cpuCount)

        batch = []
        processes = []
        batchedItems = []
        problemItems = []
        self.outputToConsole(f'OFFLOADER: Your system has {self.cpuCount} available CPUs. Processing {len(items)} {"item" if (len(items) == 1) else "items"} in {round(len(items)/loadLength) if (loadLength > 0) else 1} {"batch" if (loadLength == 0) else "batches"}...')

        i = 0

        self.updatePBar(len(items))
        
        while i < len(items):
            try:
                batch.append(items[i])
                if loadLength > 0:
                    if i % loadLength == 0 and i > 0 or i == len(items)-1:
                        self.outputToConsole(f'OFFLOADER: Compiling {len(batch)}/{len(items)}')

                        ## REMOVE ITEMS TO BE BATCHED FROM ITEMS LIST AND RESET COUNTER
                        items = [item for idx, item in enumerate(items) if idx > loadLength]
                        i = 0

                        ## GENERATE NEW QUEUES
                        QID = self.getRandInt()
                        PID = self.getRandInt()
                        self.makeQueue(QID)
                        self.makeQueue(PID)
                        
                        Que = getattr(self, f'Q{QID}')
                        PP = getattr(self, f'Q{PID}')

                        p = Process(target=updateItem, args=(Que, PP, batch, allMMIDs, allMTIDs, MMIDsOnly, MTIDsOnly,))
                        processes.append(p)

                        ## EMPTY BATCH LIST
                        batch = []

                        ## BEGIN BATCH JOB
                        p.start()

                        time.sleep(.1)

                        while not PP.empty():
                            p = PP.get()
                            self.logger.setLog(p[1])
                            self.logger.log(f'OFFLOADER: Successfully compiled a local record for {p[0]}')
                            self.updatePBar()

                        while not Que.empty():
                            q = Que.get()
                            batchedItems.append(q[0])
                            problemItems.append(q[1])
                    else:
                        i += 1
                else:
                    self.outputToConsole(f'OFFLOADER: Compiling {len(batch)}/{len(items)}')

                    ## REMOVE ITEMS TO BE BATCHED FROM ITEMS LIST AND RESET COUNTER
                    items = [item for idx, item in enumerate(items) if idx > loadLength]
                    i = 0

                    ## GENERATE NEW QUEUES
                    QID = self.getRandInt()
                    PID = self.getRandInt()
                    self.makeQueue(QID)
                    self.makeQueue(PID)
                    
                    Que = getattr(self, f'Q{QID}')
                    PP = getattr(self, f'Q{PID}')

                    p = Process(target=updateItem, args=(Que, PP, batch, allMMIDs, allMTIDs, MMIDsOnly, MTIDsOnly,))
                    processes.append(p)

                    ## EMPTY BATCH LIST
                    batch = []

                    ## BEGIN BATCH JOB
                    p.start()

                    time.sleep(.5)

                    while not PP.empty():
                        p = PP.get()
                        self.logger.setLog(p[1])
                        self.logger.log(f'OFFLOADER: Successfully compiled a local record for {p[0]}')
                        self.updatePBar()

                    while not Que.empty():
                        q = Que.get()
                        batchedItems.append(q[0])
                        problemItems.append(q[1])
            except (OSError, AttributeError) as error:
                self.outputToConsole(f'OFFLOADER ERROR: {error}. Process stopped.', False)
                break

        try:
            for process in processes:
                process.join()
        except:
            print('error with process joining!')

        try:
            problemItems = [item for items in problemItems for item in items]
            batchedItems = [item for items in batchedItems for item in items]
        except:
            print('error with converting problem items and batched items')

        try:
            i = 0
            while i < len(batchedItems):
                if not 'RenditionID' in batchedItems[i]:
                    problemItems.append(batchedItems.pop(i))
                else:
                    i += 1
        except (OSError, ValueError) as error:
            print(error)
            # print(f'{item["RenditionNumber"]} has an error with rendition ID!')
        
        try:
            if len(problemItems):
                for item in problemItems:
                    self.logger.setErrorLog(item['Origin'])
                    self.logger.logError(f'{item["RenditionNumber"]} does not appear in MMIDS or MTIDS tables and has no RenditionID associated. This file has not been further processed')
        except:
            print('error with logging problem items to log!')

        self.items = batchedItems
        self.problemRecs = problemItems

        try:
            self.newRecords = [(tuple([item['RenditionID'], 2327, item['FileName'], item['FormatID'], 'Offloader', item['File-ID'], datetime.now()])) for item in batchedItems]
        except (KeyError, OSError) as error:
            print('Error with compiling new records!', item["RenditionNumber"])
    
        self.outputToConsole(f'OFFLOADER: Local records have been updated and compiled')
        self.updatePBar(0)
        self.makeQueue(self.getRandInt(), self.DB.addMediaFiles, (self.newRecords,))

    def batchUpdate(self) -> None:
        """
        Like updateItems this method will update dependent records in memory using multiprocessing.
        """        
        # print('batchUpdate', self.items)

        items = [item for item in self.items]

        batch = []
        processes = []
        batchedItems = []
        problemItems = []

        allIIDs = self.DB.getAllIIDS()
        
        IIDsOnly = [id["RenditionID"] for id in allIIDs]

        self.outputToConsole('OFFLOADER: Retrieved IDs of new records. Batching local records...')

        loadLength = round(len(items) / self.cpuCount)

        self.updatePBar(len(items))

        # UPDATE ITEMS WITH NEW FILE IDS
        i = 0
        while i < len(items):
            try:
                batch.append(items[i])
                if loadLength == 0: loadLength = 1
                if i % loadLength == 0 and i > 0 or i == len(items)-1:
                    # self.outputToConsole(f'OFFLOADER: Loading {len(batch)}/{len(items)}')

                    ## REMOVE ITEMS TO BE BATCHED FROM ITEMS LIST AND RESET COUNTER
                    items = [item for idx, item in enumerate(items) if idx > loadLength]

                    i = 0

                    ## GENERATE NEW QUEUES
                    QID = self.getRandInt()
                    PID = self.getRandInt()
                    self.makeQueue(QID)
                    self.makeQueue(PID)
                    
                    Que = getattr(self, f'Q{QID}')
                    PP = getattr(self, f'Q{PID}')

                    p = Process(target=updateFileID, args=(Que, PP, batch, allIIDs, IIDsOnly,))
                    processes.append(p)

                    ## EMPTY BATCH LIST
                    batch = []

                    ## BEGIN BATCH JOB
                    p.start()

                    time.sleep(1)

                    while not PP.empty():
                        p = PP.get()
                        self.logger.setLog(p[1])
                        self.logger.log(f'OFFLOADER: Successfully updated local record with FileID for {p[0]}')
                        self.updatePBar()

                    while not Que.empty():
                        q = Que.get()
                        batchedItems.append(q[0])
                        problemItems.append(q[1])
                else:
                    i += 1
                # else:
                #     items = [item for idx, item in enumerate(items) if idx > loadLength]

                #     i = 0

                #     ## GENERATE NEW QUEUES
                #     QID = self.getRandInt()
                #     PID = self.getRandInt()
                #     self.makeQueue(QID)
                #     self.makeQueue(PID)
                    
                #     Que = getattr(self, f'Q{QID}')
                #     PP = getattr(self, f'Q{PID}')

                #     p = Process(target=updateFileID, args=(Que, PP, batch, allIIDs, IIDsOnly,))
                #     processes.append(p)

                #     ## EMPTY BATCH LIST
                #     batch = []

                #     ## BEGIN BATCH JOB
                #     p.start()

                #     time.sleep(1)

                #     while not PP.empty():
                #         p = PP.get()
                #         self.logger.setLog(p[1])
                #         self.logger.log(f'OFFLOADER: Successfully updated local record with FileID for {p[0]}')
                #         self.updatePBar()

                #     while not Que.empty():
                #         q = Que.get()
                #         batchedItems.append(q[0])
                #         problemItems.append(q[1])
                # # else:
                    # i += 1

            except OSError as error:
                print(error)

        self.updatePBar(0)

        try:
            for process in processes:
                process.join()
        except:
            print('error with process joining!')

        try:
            problemItems = [item for items in problemItems for item in items]
            batchedItems = [item for items in batchedItems for item in items]
        except:
            print('error with converting problem items and batched items')

        try:
            i = 0
            while i < len(batchedItems):
                if not 'FileID' in batchedItems[i]:
                    problemItems.append(batchedItems.pop(i))
                else:
                    i += 1
        except (OSError, ValueError) as error:
            print(error)

        self.items = batchedItems

        try:
            if len(problemItems):
                for item in problemItems:
                    self.logger.setErrorLog(item['Origin'])
                    self.logger.logError(f'{item["RenditionNumber"]} does not appear in IIDS table and has no FileID associated. This file has not been further processed')
        except:
            print('error with logging problem items to log!')

        self.outputToConsole('OFFLOADER: Batching successful!')

        try:
            self.newRecords = [tuple([item['FileID'], 2327, item['ThumbnailPath'] + '?width=170&height=170', 0, item['MediaMasterID']]) for item in self.items]
            # self.newRecords = [tuple([i['FileID'], 2290, i["ThumbnailPath"], i['ThumbnailPath'] + '?width=170&height=170', 0, i['MediaMasterID']]) for item in self.items for i in item]
            # self.newRecords = [tuple(item['FileID'], 2290, item['ThumbnailPath'] + '?width=170&height=170', 0, item['MediaMasterID']) for item in self.items]
            print(self.newRecords)
        except:
            print('error with newRecords!')

        self.makeQueue(self.getRandInt(), self.DB.updateMediaRenditions, (self.newRecords,))

    def verified(self) -> None:
        """
        Final stage of the processing to verify everything was done successfully
        """        
        items = [item for item in self.items]
        allMMIDs = self.DB.getAllMMIDS()
        MMIDsOnly = {}
        MMIDsOnly["RenditionNumbers"] = []
        MMIDsOnly["PrimaryFileIDs"] = []
        MMIDsOnly["RenditionNumbers"] = [id["RenditionNumber"] for id in allMMIDs]
        MMIDsOnly["PrimaryFileIDs"] = [id["PrimaryFileID"] for id in allMMIDs]

        self.updatePBar(len(self.items))
        loadLength = round(len(self.items) / self.cpuCount)

        batch = []
        processes = []
        batchedItems = []
        problemItems = []

        # UPDATE ITEMS WITH NEW FILE IDS
        i = 0
        while i < len(items):
            try:
                batch.append(items[i])
                if loadLength == 0: loadLength = 1
                if i % loadLength == 0 and i > 0 or i == len(items)-1:
                    # self.outputToConsole(f'OFFLOADER: Loading {len(batch)}/{len(items)}')

                    ## REMOVE ITEMS TO BE BATCHED FROM ITEMS LIST AND RESET COUNTER
                    items = [item for idx, item in enumerate(self.items) if idx > loadLength]

                    i = 0

                    ## GENERATE NEW QUEUES
                    QID = self.getRandInt()
                    PID = self.getRandInt()
                    self.makeQueue(QID)
                    self.makeQueue(PID)
                    
                    Que = getattr(self, f'Q{QID}')
                    PP = getattr(self, f'Q{PID}')

                    p = Process(target=verifyItem, args=(Que, PP, batch, MMIDsOnly,))
                    processes.append(p)

                    ## EMPTY BATCH LIST
                    batch = []

                    ## BEGIN BATCH JOB
                    p.start()

                    time.sleep(1)

                    while not PP.empty():
                        p = PP.get()
                        self.logger.setLog(p[1])
                        self.logger.log(f'OFFLOADER: Local copy of {p[0]} now corresponds to remote copy')
                        self.updatePBar()

                    while not Que.empty():
                        q = Que.get()
                        batchedItems.append(q[0])
                        problemItems.append(q[1])
                else:
                    i += 1
                # else:
                #     # self.outputToConsole(f'OFFLOADER: Loading {len(batch)}/{len(items)}')

                #     ## REMOVE ITEMS TO BE BATCHED FROM ITEMS LIST AND RESET COUNTER
                #     items = [item for idx, item in enumerate(self.items) if idx > loadLength]

                #     i = 0

                #     ## GENERATE NEW QUEUES
                #     QID = self.getRandInt()
                #     PID = self.getRandInt()
                #     self.makeQueue(QID)
                #     self.makeQueue(PID)
                    
                #     Que = getattr(self, f'Q{QID}')
                #     PP = getattr(self, f'Q{PID}')

                #     p = Process(target=verifyItem, args=(Que, PP, batch, MMIDsOnly,))
                #     processes.append(p)

                #     ## EMPTY BATCH LIST
                #     batch = []

                #     ## BEGIN BATCH JOB
                #     p.start()

                #     time.sleep(1)

                #     while not PP.empty():
                #         p = PP.get()
                #         self.logger.setLog(p[1])
                #         self.logger.log(f'OFFLOADER: Local copy of {p[0]} now corresponds to remote copy')
                #         self.updatePBar()

                #     while not Que.empty():
                #         q = Que.get()
                #         batchedItems.append(q[0])
                #         problemItems.append(q[1])
            except OSError as error:
                print(error)
        
        try:
            if len(problemItems[0]):
                for idx, item in enumerate(problemItems):
                    self.problemRecs.append(batchedItems.pop(idx))
                    self.logger.setErrorLog(item[0]['Origin'])
                    self.logger.logError(f'{item[0]["RenditionNumber"]} does not appear in MMIDS or MTIDS tables and has no RenditionID associated. This file has not been further processed')
        except OSError as error:
            print('error with logging problem items to log!')

        self.updatePBar(0)

        self.outputToConsole(f'OFFLOADER: {len(self.problemRecs)} records from the files you selected have not been updated. Check the error logs for more information!')

        self.outputToConsole(">>>>>> CLEANING UP <<<<<<", True)

        self.finishProcessing()

    def finishProcessing(self) -> None:
        """
        Finish up after processing is complete: 
        - drop scratchtable
        - remove local instance of TinyDB with data
        - reset progress bar
        - release toggle bindings
        - update user
        """        
        self.DB.dropScratchTable()
        self.DB.removeTinyDB()
        self.updatePBar()
        self.toggleBindings('Control', ['p', 'o', 'l', 'm', 'g', 'u', 'v', 's'])
        self.outputToConsole('>>>>>> ALL DONE! <<<<<<', True)

    def getRandInt(self) -> None: 
        """
        Returns a random integer that is not yet used by the queues to process data

        Returns:
            int: some random integer
        """        
        i = random.randint(1, 100000)
        if i not in self.Queues:
            self.Queues.append(i)
            return i
        else:
            self.getRandInt()

    def makeQueue(self, idx:int, func:function=None, args:tuple=()) -> None:
        """
        This function makes a queue and starts the function assigned to it in a separate ThreadedTask

        Args:
            idx (int): Some random integer for a new Queue instance
            func (function, optional): function to be run for that Queue. Defaults to None.
            args (tuple, optional): arguments to be passed into the function. Defaults to ().
        """        
        try:
            Q = f'Q{idx}'
            setattr(self, Q, Queue())
            setattr(self, f'{Q}fn', func)
            self.Qs.append(getattr(self, Q))
            if func is not None:
                ThreadedTask(getattr(self, Q), self, getattr(self, f'{Q}fn'), args).start()
                self.checkQueue(idx)
        except:
            print('Error in queue construction')

    def checkQueue(self, idx:int) -> None:
        """
        Processes the result of ThreadedTask. Result is a string that identifies the next stage in the batch process pipeline.

        Args:
            idx (int): number of the queue
        """
        try:
            Q = f'Q{idx}'
            Que = getattr(self, Q)
            QResult = Que.get(0)
            for attr in (Q, f'{Q}fn'):
                self.__dict__.pop(attr, None)
            self.Qs.pop(self.Qs.index(Que))
            self.Queues.pop(self.Queues.index(idx))
            if QResult is not None:
                if 'getMMIDs' in QResult: self.makeQueue(self.getRandInt(), self.DB.getMMIDs)
                if 'updateItems' in QResult: self.makeQueue(self.getRandInt(), self.updateItems)
                if 'getIIDs' in QResult: self.makeQueue(self.getRandInt(), self.DB.getIIDs)
                if 'batchUpdate' in QResult: self.makeQueue(self.getRandInt(), self.batchUpdate)
                if 'verify' in QResult: self.makeQueue(self.getRandInt(), self.DB.getMMIDs, (True,))
                if 'verified' in QResult: self.makeQueue(self.getRandInt(), self.verified)
            # if QResult is not None and '()' in QResult:
                # self.makeQueue(self.getRandInt(), exec(QResult))
        except Empty:
            self.after(100, self.checkQueue, idx)

    def outputToConsole(self, msg:str, res:str/bool='normal') -> None:
        """
        Output the result of the operation to the console

        Args:
            msg (str): Message to be displayed to the user
            res (str/bool, optional): Controls the color of the text in the output to draw attention for the user. Defaults to 'normal'. Other options include bool values True or False.
        """        
        self.logger.logToSystemLog(msg)
        
        self.OutputTextBox.tag_configure(True, foreground="green")
        self.OutputTextBox.tag_configure(False, foreground="red")
        self.OutputTextBox.tag_configure('normal', foreground="black")

        self.OutputTextBox.config(state=NORMAL)
        self.OutputTextBox.insert(END, f'[{datetime.now().strftime("%X")}] {msg}\n', res)
        self.OutputTextBox.config(state=DISABLED)
        self.OutputTextBox.see(END)

    def exit(self) -> None:
        """
        Save the state of the program and exit
        """        
        self.save()
        self.logger.log('Program finished with code 0')
        exit(0)

if __name__ == '__main__':

    app = GUIConverter()
    app.mainloop()