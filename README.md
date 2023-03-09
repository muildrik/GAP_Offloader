# About the Giza Archives Project
This is the code base for an application I developed for the Giza Archives Project at Harvard University. The Giza Archives Project aims to make archival documentation available on excavations at the Giza plateau in Egypt during the first half of the 20th century by the American archaeologist, George Andrew Reisner. Reisner worked for the joint Harvard University-Boston Museum of Fine Arts Egyptian Expedition and in the 1940s, after his death in 1942, most of his documentation was transferred to the Museum of Fine Arts in Boston. Over the course of the second half of the 20th century up to the present this documentation provides a valuable resource to understanding the extensive excavations undertaken on the Giza plateau. In the early 2000s much of the documentation was catalogued and digitized in The Museum System (TMS), a proprietary software platform developed by Gallery Systems used in many different museum settings. The Giza Archives Project is reliant on TMS for its day-to-day operations, but has not yet taken the necessary steps to upgrade to the most recent version.

# About this application
In 2020 the Giza Archives Project at Harvard University employed me to design, develop and deploy an application that would enable them to batch process records in their version of TMS, particularly with respect to updating relative paths of digital assets. This bulk update was necessary because Harvard University's IT infrastructure had allocated new storage space for their digital assets at the library with Digital Scholarly Resources (DRS) that would enable more sophisticated access to this archaeological data. DRS generates reports during depositing new media files, which provides data that relate each image to key information in TMS.

## Purpose
The application was developed in Python 3.7 at the time. The version in this repository has been developed with Python 3.10 in a virtual environment on a Windows 11 machine running VSCode. The application is a standalone Windows GUI batch processing application that takes one or multiple DRS reports and extracts TMS data. The application also downloads data spread across various TMS tables made available via a Microsoft Server instance by FAS Research Computing.

## Design
Below is a brief overview of the application, roughly how it processes data, and explains the different files in outline.

### Overview
The application takes in one or multiple DRS reports (an example is included in this repo) and per file extracts key pieces of data, including an object ID (OBJ-ID), the Uniform Resource Name (OBJ-DELIV-URN) and original TMS object name (OBJ-OSN) for each asset deposited with DRS. Each DRS report contains each entry in duplicate, once with an .xml-file and once with a .jp2-file, and during the extraction process file-format is used to eliminate extraneous duplicates. The processing begins with an initial download of all required tables from the MSSQL instance. These tables can be stored for later reuse, to prevent unnecessary continuous calls to the server, but will be purged if inconsistencies are encountered and will need to be downloaded again. They are downloaded in a multithreading operation to speed up the process and on a slow connection may take up to ten minutes. The data extracted from the DRS reports is then mutated in a multiprocessing pool with these tables to update file paths in memory. These can be dumped to files on disk as well and will be pushed in a multithreading operation, in bulk, to directly update the tables in the MSSQL instance. The program keeps log files of all its jobs to enable the user to trace problems.

### Interface
The GUI was built in Python's builtin GUI library, Tkinter. My original goal was to have a command line interface as well as a Windows GUI. However, the application was going to be run by personel at the Giza Archives Project on Windows machines and therefore a command line would not be required; a Windows GUI was the only requirement. The application can be run with 'main.py' from a Python command line. Main.py will call the mainloop on the Tkinter elements in GUIConverter.py and open the GUI. There are 

### Files
In addition to main.py and GUIConverter.py there are seven additional files that each configure a class:
- ThreadedTask in Thread.py
- Settings in Settings.py
- Backup in Backup.py
- Logger in Logger.py
- ErrorHandler in ErrorHandler.py
- DB in DB.py
- Extractor in Extractor.py

These seven classes are called in GUIConverter.py and maintain specific tasks within the program.

#### ThreadedTask
This class provides new instances for multithreading queues.

#### Settings
This class stores the settings of the program in a local SQLite database.

#### Backup
This small class serves as interface to store data on the local disk.

#### Logger
This class starts new log files and documents the process of the application.

#### ErrorHandler
This class provides custom handling of various errors that could occur during processing.

#### DB
This class provides database interaction with the MSSQL instance. The class uses a dynamic cursor to enable multiple connections for multithreading to be made with the MSSQL instance. The class will initially check if the database connection is working and then the main methods (called from main.py) then proceed to download different tables from the MSSQL instance.

#### Extractor
This class extracts all important data from each DRS report and reformats this data into a single uniform record with all important pieces of data.

### GUIConverter.py
The bulk of the work happens in GUIConverter.py. The above classes are loaded and accessed as necessary via user interaction. See each file for respective functionality documented throughout.

## Deployment
The Giza Archives Project required a single executable to run this program and was therefore compiled with 'pyinstaller main.py --onefile'.

## TODO
The software has been deployed, however, in future iterations error handling should be better organized as well as the logging of different aspects in the application.