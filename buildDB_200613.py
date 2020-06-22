import pdb
import sys 
#sys.path.append('/global/homes/s/sgkim/pytokio-master')
sys.path.append('/global/project/projectdirs/m1248/cilioh14/pytokio')
import time
import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams.update({'font.size': 14})
import datetime
import tokio.tools
import pandas
import math
import os
import multiprocessing
from multiprocessing import Manager, Process
import sqlite3
import re
import datetime
import subprocess
import traceback
#import scandir

from imp import reload
reload(sys)

#os.chdir("/global/u2/s/sgkim/codes/darshan_code_skim/")
#dir = "/global/cscratch1/sd/tengwang/miner0612/parsed_darshan/2017"
#dir = "/global/project/projectdirs/m1248/sgkim/h5write"
#dir = "/global/project/projectdirs/m1248/sgkim/parsed_darshan"
dir = "/global/cscratch1/sd/sbyna/logs/darshan/Cori_archive_2018/parsed_darshan"
#localdir = "/global/u2/s/sgkim/codes/darshan_code_skim/"
localdir = "/global/project/projectdirs/m1248/cilioh14/darshan_data/"
app = "total"
#sqlite cannot accpet - as table name
app_sqlite = app.replace('-','_')
#set ouput dir
outputdir = localdir + app_sqlite + ".sql"

#delete existing file
if os.path.exists(localdir + outputdir):
        os.remove(localdir + outputdir) 

if os.path.exists(localdir + app_sqlite + ".db"):
        os.remove(localdir + app_sqlite + ".db") 

if os.path.exists(localdir + "skipped.txt"):
        os.remove(localdir + "skipped.txt")

conn = sqlite3.connect(localdir + app_sqlite + ".db")

c = conn.cursor()
c.execute("DROP TABLE IF EXISTS " + app_sqlite)
c.execute('CREATE TABLE ' + app_sqlite
          + """ (darshanDir text, progName text, userID text, jobID int, startTime DATETIME, endTime DATETIME, ioStartTime int, runTime int, numProc int, numCPU int, numNode int,
  numOST int, stripeSize int, knl int,
  totalFile int, totalIOReqPOSIX int, totalMetaReqPOSIX int,
  totalIOReqMPIIO int, totalMetaReqMPIIO int, totalIOReqSTDIO int, totalMetaReqSTDIO int,
  mdsCPUMean float, mdsCPU95 float, mdsOPSMean float, mdsOPSMin float, mdsOPS95 float, ossReadMean float, ossRead95 float, ossWriteMean float, ossWrite95 float, 
  ossReadMeanUsed float, ossRead95Used float, ossWriteMeanUsed float, ossWrite95Used float, ossWriteLargestUsed float, ossReadLargestUsed float,
  ossReadHigher1g float, ossReadHigher4g float, ossWriteHigher1g float, ossWriteHigher4g float,
  totalFilePOSIX int, totalFileMPIIO int, totalFileSTDIO int, seqWritePct float, seqReadPct float, consecWritePct float, consecReadpct float,
  writeLess1k float, writeMore1k float, readLess1k float, readMore1k float,
  writeLess1m float, writeMore1m float, readLess1m float, readMore1m float,
  writeBytesTotal float, readBytesTotal float, writeRateTotal float, readRateTotal float,
  writeBytesPOSIX float, writeTimePOSIX float, writeRatePOSIX float, readBytesPOSIX float, readTimePOSIX int, readRatePOSIX float,
  writeBytesMPIIO float, writeTimeMPIIO float, writeRateMPIIO float, readBytesMPIIO float, readTimeMPIIO int, readRateMPIIO float,
  writeBytesSTDIO float, writeTimeSTDIO float, writeRateSTDIO float, readBytesSTDIO float, readTimeSTDIO int, readRateSTDIO float,
  ReadReqPOSIX int, WriteReqPOSIX int, OpenReqPOSIX int, SeekReqPOSIX int, StatReqPOSIX int, readTimePOSIXonly float, writeTimePOSIXonly float, readTimeMPIIOonly float, writeTimeMPIIOonly float, readTimeSTDIOonly float, writeTimeSTDIOonly float, metaTimePOSIX float, metaTimeMPIIO float, metaTimeSTDIO float,
  slowWriteTimePOSIX float, slowReadTimePOSIX float, slowWriteTimeMPIIO float, slowReadTimeMPIIO float, IndOpenReqMPIIO int, ColOpenReqMPIIO int, IndWriteReqMPIIO int, ColWriteReqMPIIO int, IndReadReqMPIIO int, ColReadReqMPIIO int,
  SplitReadReqMPIIO int, SplitWriteReqMPIIO int, NbReadReqMPIIO int, NbWriteReqMPIIO int,
  OpenReqMPIIO int, ReadReqMPIIO int, WriteReqMPIIO int,
  OpenReqSTDIO int, ReadReqSTDIO int, WriteReqSTDIO int, SeekReqSTDIO int, FlushReqSTDIO int,
  ostlist text, ossWriteLargest float, ossReadLargest float, ossWriteStart95 float, ossWriteStartLargest float, writeBytesPct float)""")


mdsCPU_df = mdsOPS_df = ossRead_df = ossWrite_df = 0 

result_list = []
def process(file):
        fullDir = file
        file = os.path.basename(file)
        """
        if file.endswith(".darshan"):
            command = "darshan-parser --all " + fullDir + " > " + fullDir + ".all"
            os.system(command)
            print(command)
            file = file+".all"
            fullDir = fullDir+".all"
            print(file)
            print(fullDir)
        """
        if file.endswith(".all"):
                #Check filename with application name
                try:
                        tempID = re.findall(r"id+\d+", os.path.basename(file))[0]
                except:
                        print ("skipping" + file)
                        return
                #if os.path.basename(file)[file.find("_") + 1 : file.find(tempID) - 1] == app:
                if 1:
                        progName = userID = jobID = startTime = endTime = runTime = numProc = numOST = stripeSize  = -1
                        ioStartTime = -1
                        totalFilePOSIX = totalFileMPIIO = totalFileSTDIO = 0
                        writeRatePOSIX = readRatePOSIX = 0
                        writeRateSTDIO = readRateSTDIO = 0
                        writeRateMPIIO = readRateMPIIO = 0
                        writeBytesPOSIX = writeTimePOSIX = readBytesPOSIX = readTimePOSIX =  0
                        writeBytesMPIIO = writeTimeMPIIO = readBytesMPIIO = readTimeMPIIO =  0
                        writeBytesSTDIO = writeTimeSTDIO = readBytesSTDIO = readTimeSTDIO =  0
                        slowWriteTimePOSIX = slowReadTimePOSIX = 0
                        slowWriteTimeMPIIO = slowReadTimeMPIIO = 0
                        IndOpenReqMPIIO = ColOpenReqMPIIO = IndWriteReqMPIIO = ColWriteReqMPIIO = IndReadReqMPIIO = ColReadReqMPIIO = 0
                        SplitReadReqMPIIO = SplitWriteReqMPIIO = NbReadReqMPIIO = NbWriteReqMPIIO = 0
                        OpenReqMPIIO = ReadReqMPIIO = WriteReqMPIIO = 0
                        OpenReqSTDIO = WriteReqSTDIO = ReadReqSTDIO = SeekReqSTDIO = FlushReqSTDIO = 0
                        writeBytesTotal = writeRateTotal = 0
                        metaTimePOSIX = metaTimeMPIIO = metaTimeSTDIO = 0
                        readBytesTotal = readRateTotal = 0
                        WriteReqPOSIX = ReadReqPOSIX = 0        
                        seqWriteReqPOSIX = seqReadReqPOSIX = 0    
                        seqWritePct = seqReadPct = 0
                        consecWriteReqPOSIX = consecReadReqPOSIX = 0
                        consecWritePct = consecReadPct = 0
                        readLess1k = readMore1k = writeLess1k = writeMore1k = 0
                        readLess1m = readMore1m = writeLess1m = writeMore1m = 0
                        OpenReqPOSIX = SeekReqPOSIX = StatReqPOSIX = 0
                        readTimePOSIXonly = writeTimePOSIXonly = 0
                        readTimeMPIIOonly = writeTimeMPIIOonly = 0
                        readTimeSTDIOonly = writeTimeSTDIOonly = 0
                        totalFile = totalIOReqPOSIX = totalMetaReqPOSIX = 0
                        totalIOReqMPIIO = totalMetaReqMPIIO = totalIOReqSTDIO = totalMetaReqSTDIO = 0
                        isknl = 0;
                        usedOST = list()                                        
                        ostlist = ""
                        writeBytesPct = 0
                        progName = os.path.basename(file)[file.find("_") + 1 : file.find(tempID) - 1] 
                        userID = os.path.basename(file)[:file.find("_")]
                        jobID = re.findall(r"id+\d+", os.path.basename(file))[0][2:]
                        #jiwoo
                        #print(progName + " " + userID + " " + jobID)
                        """
                        try:
                            #f = open(fullDir , "rb", 32768 )
                            f = open(fullDir, "r", 32768)
                        except:
                            print("ERROR")
                        """
                        with open(fullDir, 'r', encoding='utf-8') as f:
                            for line in f:
                                #print(line)
                                if "start_time_asci: " in line:
                                    startTime = line.split(": ",1)[1][:-1] 
                                    temp = re.findall(r'\S+', startTime)
                                    startTime = temp[1] + " " + temp[2] + " " +  temp[3] + " " + temp[4]
                                    startTime = datetime.datetime.strptime(startTime, '%b %d %H:%M:%S %Y')
                                if "run time: " in line:
                                    runTime = line.split(": ",1)[1][:-1]
                                if "nprocs: " in line:
                                    numProc = line.split(": ",1)[1][:-1]
                                if ("total_POSIX_F_OPEN_START_TIMESTAMP: " in line) or ("total_POSIX_F_READ_START_TIMESTAMP: " in line) or ("total_POSIX_F_WRITE_START_TIMESTAMP: " in line):
                                    if ioStartTime == -1 or ioStartTime > float(line.split(" ")[1]):
                                        ioStartTime = float(line.split(" ")[1])
                                if "total: " in line:
                                    if int(line.split(" ")[3]) == int((writeBytesPOSIX + readBytesPOSIX)*1000000):
                                        totalFilePOSIX = line.split(" ")[2]
                                    elif int(line.split(" ")[3]) == int((writeBytesMPIIO + readBytesMPIIO)*1000000):
                                        totalFileMPIIO = line.split(" ")[2]
                                    elif int(line.split(" ")[3]) == int((writeBytesSTDIO + readBytesSTDIO)*1000000):
                                        totalFileSTDIO = line.split(" ")[2]     
                                if "LUSTRE_OST_ID_" in line and not line.startswith("#"):
                                    temp = int(line.split("\t")[3][14:]) + 1
                                    #set numOST as the highest OST
                                    #numOST may vary accroding to file or directory
                                    if int(line.split("\t")[4]) not in usedOST:
                                        usedOST.append(int(line.split("\t")[4]))
                                if "LUSTRE_STRIPE_SIZE" in line and not line.startswith("#"):
                                    temp = int(line.split("\t")[4])
                                    #set stripeSize as the max stripe sizeT
                                    #stripesize may vary accroding to file or directory
                                    if temp > stripeSize:
                                        stripeSize = temp
                                #POSIX request Size
                                if "total_POSIX_SIZE_READ_0_100: " in line:
                                    readLess1m = readLess1m + int(line.split(" ")[1])
                                    readLess1k = readLess1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_READ_100_1K: " in line:
                                    readLess1m = readLess1m + int(line.split(" ")[1])
                                    readLess1k = readLess1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_READ_1K_10K: " in line:
                                    readLess1m = readLess1m + int(line.split(" ")[1])
                                    readMore1k = readMore1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_READ_10K_100K: " in line:
                                    readLess1m = readLess1m + int(line.split(" ")[1])
                                    readMore1k = readMore1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_READ_100K_1M: " in line:
                                    readLess1m = readLess1m + int(line.split(" ")[1])
                                    readMore1k = readMore1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_READ" in line:
                                    readMore1m = readMore1m + int(line.split(" ")[1])
                                    readMore1k = readMore1k + int(line.split(" ")[1])
                                if "total_POSIX_SIZE_WRITE_0_100: " in line:
                                    writeLess1m = writeLess1m + int(line.split(" ")[1])
                                    writeLess1k = writeLess1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_WRITE_100_1K: " in line:
                                    writeLess1m = writeLess1m + int(line.split(" ")[1])
                                    writeLess1k = writeLess1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_WRITE_1K_10K: " in line:
                                    writeLess1m = writeLess1m + int(line.split(" ")[1])
                                    writeLess1k = writeLess1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_WRITE_10K_100K: " in line:
                                    writeLess1m = writeLess1m + int(line.split(" ")[1])
                                    writeMore1k = writeMore1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_WRITE_100K_1M: " in line:
                                    writeLess1m = writeLess1m + int(line.split(" ")[1])
                                    writeMore1k = writeMore1k + int(line.split(" ")[1])
                                elif "total_POSIX_SIZE_WRITE" in line:
                                    writeMore1m = writeMore1m + int(line.split(" ")[1])
                                    writeMore1k = writeMore1k + int(line.split(" ")[1])

                                #ADD METATIME to write/read time        
                                if "total_POSIX_F_META_TIME: " in line and not line.startswith("#"):
                                    metaTimePOSIX = float(line.split(" ")[1])
                                #jiwoo
                                if "total_MPIIO_F_META_TIME: " in line and not line.startswith("#"):
                                    metaTimeMPIIO = float(line.split(" ")[1])
                                if "total_STDIO_F_META_TIME: " in line and not line.startswith("#"):
                                    metaTimeSTDIO = float(line.split(" ")[1])

                                #POSIX I/O Rate
                                #writeRatePOSIX
                                if "total_POSIX_BYTES_WRITTEN:" in line and not line.startswith("#"):
                                    writeBytesPOSIX = float(line.split(" ")[1])/1000000
                                    #TEST only go through write bytes POSIX > 10G
                                #jiwoo
                                if "total_POSIX_F_WRITE_TIME:" in line and not line.startswith("#"):
                                    writeTimePOSIX = float(line.split(" ")[1])
                                    writeTimePOSIXonly = writeTimePOSIX
                                if "total_POSIX_F_MAX_WRITE_TIME:" in line and not line.startswith("#"):
                                    slowWriteTimePOSIX = float(line.split(" ")[1])
                                    #writeTimePOSIXonly = writeTimePOSIX
                                #readRatePOSIX
                                if "total_POSIX_BYTES_READ:" in line and not line.startswith("#"):
                                    readBytesPOSIX = float(line.split(" ")[1])/1000000
                                #jiwoo
                                if "total_POSIX_F_READ_TIME:" in line and not line.startswith("#"):
                                    readTimePOSIX = float(line.split(" ")[1])
                                    readTimePOSIXonly = readTimePOSIX
                                if "total_POSIX_F_MAX_READ_TIME:" in line and not line.startswith("#"):
                                    slowReadTimePOSIX = float(line.split(" ")[1])
                                    #readTimePOSIXonly = readTimePOSIX
                                
                                #MPIIO I/O Rate
                                #writeRateMPIIO
                                if "total_MPIIO_BYTES_WRITTEN:" in line and not line.startswith("#"):
                                    writeBytesMPIIO = float(line.split(" ")[1])/1000000
                                if "total_MPIIO_F_WRITE_TIME:" in line and not line.startswith("#"):
                                    writeTimeMPIIO = float(line.split(" ")[1])
                                    writeTimeMPIIOonly = writeTimeMPIIO
                                if "total_MPIIO_F_MAX_WRITE_TIME:" in line and not line.startswith("#"):
                                    slowWriteTimeMPIIO = float(line.split(" ")[1])
                                #readRateMPIIO
                                if "total_MPIIO_BYTES_READ:" in line and not line.startswith("#"):
                                    readBytesMPIIO = float(line.split(" ")[1])/1000000
                                if "total_MPIIO_F_READ_TIME:" in line and not line.startswith("#"):
                                    readTimeMPIIO = float(line.split(" ")[1])
                                    readTimeMPIIOonly = readTimeMPIIO
                                if "total_MPIIO_F_MAX_READ_TIME:" in line and not line.startswith("#"):
                                    slowReadTimeMPIIO = float(line.split(" ")[1])

                                #jiwoo
                                #MPIIO metadata request
                                if "total_MPIIO_INDEP_OPENS:" in line and not line.startswith("#"):
                                    IndOpenReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_COLL_OPENS:" in line and not line.startswith("#"):
                                    ColOpenReqMPIIO = float(line.split(" ")[1])
                                
                                #MPIIO I/O request
                                if "total_MPIIO_INDEP_WRITES:" in line and not line.startswith("#"):
                                    IndWriteReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_COLL_WRITES:" in line and not line.startswith("#"):
                                    ColWriteReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_INDEP_READS:" in line and not line.startswith("#"):
                                    IndReadReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_COLL_READS:" in line and not line.startswith("#"):
                                    ColReadReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_SPLIT_READS:" in line and not line.startswith("#"):
                                    SplitReadReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_SPLIT_WRITES:" in line and not line.startswith("#"):
                                    SplitWriteReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_NB_READS:" in line and not line.startswith("#"):
                                    NbReadReqMPIIO = float(line.split(" ")[1])
                                if "total_MPIIO_NB_WRITES:" in line and not line.startswith("#"):
                                    NbWriteReqMPIIO = float(line.split(" ")[1])

                                #STDIO I/O Rate
                                #writeRateSTDIO
                                if "total_STDIO_BYTES_WRITTEN:" in line and not line.startswith("#"):
                                    writeBytesSTDIO = float(line.split(" ")[1])/1000000
                                if "total_STDIO_F_WRITE_TIME:" in line and not line.startswith("#"):
                                    writeTimeSTDIO = float(line.split(" ")[1])
                                    writeTimeSTDIOonly = writeTimeSTDIO
                                #readRateSTDIO
                                if "total_STDIO_BYTES_READ:" in line and not line.startswith("#"):
                                    readBytesSTDIO = float(line.split(" ")[1])/1000000
                                if "total_STDIO_F_READ_TIME:" in line and not line.startswith("#"):
                                    readTimeSTDIO = float(line.split(" ")[1])
                                    readTimeSTDIOonly = readTimeSTDIO

                                #jiwoo
                                #STDIO I/O request
                                if "total_STDIO_OPENS:" in line and not line.startswith("#"):
                                    OpenReqSTDIO = float(line.split(" ")[1])
                                if "total_STDIO_WRITES:" in line and not line.startswith("#"):
                                    WriteReqSTDIO = float(line.split(" ")[1])
                                if "total_STDIO_READS:" in line and not line.startswith("#"):
                                    ReadReqSTDIO = float(line.split(" ")[1])
                                if "total_STDIO_SEEKS:" in line and not line.startswith("#"):
                                    SeekReqSTDIO = float(line.split(" ")[1])
                                if "total_STDIO_FLUSHES:" in line and not line.startswith("#"):
                                    FlushReqSTDIO = float(line.split(" ")[1])

                                #POSIX seq request
                                if "total_POSIX_WRITES:" in line and not line.startswith("#"):
                                    WriteReqPOSIX = float(line.split(" ")[1])
                                if "total_POSIX_SEQ_WRITES:" in line and not line.startswith("#"):
                                    seqWriteReqPOSIX = float(line.split(" ")[1])
                                if 'total_POSIX_CONSEC_WRITES:' in line and not line.startswith('#'):
                                    consecWriteReqPOSIX = float(line.split(' ')[1])

                                if "total_POSIX_READS" in line and not line.startswith("#"):
                                    ReadReqPOSIX = float(line.split(" ")[1])
                                if "total_POSIX_SEQ_READS:" in line and not line.startswith("#"):
                                    seqReadReqPOSIX = float(line.split(" ")[1])
                                if 'total_POSIX_CONSEC_READS:' in line and not line.startswith('#'):
                                    consecReadReqPOSIX = float(line.split(' ')[1])

                                #POSIX metadata request
                                if "total_POSIX_OPENS:" in line and not line.startswith("#"):
                                    OpenReqPOSIX = float(line.split(" ")[1])
                                if "total_POSIX_SEEKS:" in line and not line.startswith("#"):
                                    SeekReqPOSIX = float(line.split(" ")[1])
                                if "total_POSIX_STATS:" in line and not line.startswith("#"):
                                    StatReqPOSIX = float(line.split(" ")[1])
                                if "# agg_perf_by_slowest" in line and line.startswith("#"):
                                    writeRateTotal = float(line.split(" ")[2])
                                #byfile?
                                if "agg_perf_by_slowest:" in line and line.startswith("#"):
                                    if writeRatePOSIX == 0:
                                        writeRatePOSIX = float(line.split(" ")[2])
                                    elif writeRateMPIIO == 0:
                                        writeRateMPIIO = float(line.split(" ")[2])
                                    else:
                                        writeRateSTDIO = float(line.split(" ")[2])

                        #if(writeBytesPOSIX == 0):
                        #       return

                        #END LINE loop
                        ReadReqMPIIO = IndReadReqMPIIO + ColReadReqMPIIO
                        WriteReqMPIIO = IndWriteReqMPIIO + ColWriteReqMPIIO
                        OpenReqMPIIO = IndOpenReqMPIIO + ColOpenReqMPIIO

                        writeTimePOSIX = metaTimePOSIX + writeTimePOSIX
                        readTimePOSIX = metaTimePOSIX + readTimePOSIX
                        
                        writeTimeMPIIO = metaTimeMPIIO + writeTimeMPIIO
                        readTimeMPIIO = metaTimeMPIIO + writeTimeMPIIO

                        writeTimeSTDIO = metaTimeSTDIO + writeTimeSTDIO
                        readTimeSTDIO = metaTimeSTDIO + writeTimeSTDIO
                        
                        #calculate rates after file iteration
                        #MB/s   

                        if writeBytesPOSIX != 0 and writeRatePOSIX == 0:
                            writeRatePOSIX = float(writeBytesPOSIX/writeTimePOSIX)
                        if readBytesPOSIX != 0:
                            readRatePOSIX = readBytesPOSIX/readTimePOSIX
                        if writeBytesMPIIO != 0 and writeRateMPIIO == 0:               
                            writeRateMPIIO = writeBytesMPIIO/writeTimeMPIIO
                        if readBytesMPIIO != 0:
                            readRateMPIIO = readBytesMPIIO/readTimeMPIIO
                        if writeBytesSTDIO != 0 and writeRateSTDIO == 0:                
                            writeRateSTDIO = writeBytesSTDIO/writeTimeSTDIO
                        if readBytesSTDIO != 0:
                            readRateSTDIO = readBytesSTDIO/readTimeSTDIO
                        #calculate total read/write btyes
                        #MB/s
                        writeBytesTotal = float(writeBytesPOSIX + writeBytesMPIIO + writeBytesSTDIO)
                        #1GB
                        #jiwoo
                        if(writeBytesTotal < 1000):
                            return  
                        readBytesTotal = float(readBytesPOSIX + readBytesMPIIO + readBytesSTDIO)
                        writeRateTotal = (writeRatePOSIX + writeRateMPIIO + writeRateSTDIO)
                        #readRateTotal = (readRatePOSIX + readRateMPIIO + readRateSTDIO)
                        if WriteReqPOSIX != 0:
                            seqWritePct = (seqWriteReqPOSIX/WriteReqPOSIX) * 100
                            consecWritePct = consecWriteReqPOSIX / WriteReqPOSIX * 100
                        if ReadReqPOSIX != 0:
                            seqReadPct = (seqReadReqPOSIX/ReadReqPOSIX) * 100
                            consecReadPct = consecReadReqPOSIX / ReadReqPOSIX * 100      
                        #in some cases consecWrite or seqWrite can be over 100, higher than num write request
                        #just set them as 100
                        if consecWritePct > 100:
                            consecWritePct = 100
                        if seqWritePct > 100:
                            seqWritePct = 100
                        if consecReadPct > 100:
                            consecReadPct = 100
                        if seqReadPct > 100:
                            seqReadPct = 100
                        totalWriteTemp = writeLess1m + writeMore1m
                        if totalWriteTemp != 0: 
                            writeLess1m = (writeLess1m/totalWriteTemp) * 100
                            writeLess1k = (writeLess1k/totalWriteTemp) * 100
                            writeMore1m = (writeMore1m/totalWriteTemp) * 100
                            writeMore1k = (writeMore1k/totalWriteTemp) * 100
                        totalReadTemp = readLess1m + readMore1m
                        if totalReadTemp != 0:
                            readLess1m = (readLess1m/totalReadTemp) * 100
                            readLess1k = (readLess1k/totalReadTemp) * 100
                            readMore1m = (readMore1m/totalReadTemp) * 100
                            readMore1k = (readMore1k/totalReadTemp) * 100
                        try:
                            endTime = startTime + datetime.timedelta(seconds=int(runTime))  
                        except:
                            endTime = "-1"
                        #In Case of no POSIX, just set start IO time to 0
                        if ioStartTime == -1:
                            ioStartTime = 0
                        totalFile = int(totalFilePOSIX) + int(totalFileMPIIO) + int(totalFileSTDIO)
                        totalIOReqPOSIX = int(ReadReqPOSIX) + int(WriteReqPOSIX)
                        totalMetaReqPOSIX = int(OpenReqPOSIX) + int(SeekReqPOSIX) + int(StatReqPOSIX) 
                        totalIOReqMPIIO = int(ReadReqMPIIO) + int(WriteReqMPIIO)
                        totalMetaMPIIO = int(OpenReqMPIIO)
                        totalIOReqSTDIO = int(ReadReqSTDIO) + int(WriteReqSTDIO)
                        totalMetaSTDIO = int(OpenReqSTDIO) + int(SeekReqSTDIO)

                        #get numCPU from SLURM
                        #figure out which th darshanfile with same jobID (to match SLURM)
                        cmd = "sacct -j " + str(jobID) + " --format=ncpus,nnodes,qos"
                        '''
                        try:
                                tempOut =  subprocess.check_output(cmd, shell=True)
                                tempOut = tempOut.strip()
                                tempOut = tempOut.split(" ")
                                temp = []
                                for i in tempOut:
                                        if i.isdigit():
                                                temp.append(i)
                                        if "kn" in i:
                                                isknl = 1;      
                                numCPU = int(temp[0])
                                numNode = int(temp[1])
                        except:
                                isknl = 0
                                numCPU = 0
                                numNode = 0     
                        '''
                        isknl = 0
                        numCPU = 0
                        numNode = 0
                        usedOSTName = []

                        if usedOST != []:
                            numOST = 0
                            try:
                                for ost in usedOST:
                                    numOST = numOST + 1
                                    ostlist = ostlist + " " + str(ost)
                                    tempstr = "snx11168-OST" + str(format(ost, '04x')) 
                                    usedOSTName.append(tempstr)
                                ostlist = ostlist[1:]
                                ostlist = "'%s'" % ostlist
                            except:
                                pass
                        else:
                            ostlist = "'-1'"

                        if numOST == -1:
                            return
                        #print(usedOSTName)
                        mdsCPUMean = mdsCPU95 = mdsOPSMin = mdsOPSMean = mdsOPS95 = ossReadMean = ossRead95 = ossWriteMean = ossWrite95 = 0 
                        ossReadHigher1g = ossReadHigher4g = ossWriteHigher1g = ossWriteHigher4g = 0
                        ossWriteLargest = ossReadLargest = 0
                        ossReadMeanUsed = ossRead95Used = ossWriteMeanUsed = ossWrite95Used = ossWriteLargestUsed = ossReadLargestUsed = 0
                        ossWriteStart95 = ossWriteStartLargest = 0
                        #if numCPU != 0:
                        if isinstance(mdsCPU_df, pandas.core.frame.DataFrame) and writeTimePOSIX != 0:
                                try:    
                                        #if run time is too short, there might be no data to read
                                        #lmtStartTime = startTime - datetime.timedelta(seconds=5)
                                        lmtStartTime = startTime 
                                        lmtEndTime = startTime + datetime.timedelta(seconds=int(runTime))
                                        df = ossWrite_df.loc[lmtStartTime-datetime.timedelta(seconds=int(5)):lmtStartTime]
                                        df.columns = [x.decode("utf-8") for x in df.columns]
                                        df = df.loc[:, usedOSTName]
                                        for column in df:
                                                if df[column].mean() > ossWriteStartLargest:
                                                        ossWriteStartLargest = df[column].mean()
                                        df = ossWrite_df.loc[lmtStartTime-datetime.timedelta(seconds=int(5)):lmtStartTime]
                                        df.columns = [x.decode("utf-8") for x in df.columns]
                                        df = df.sum(axis = 1)
                                        i=0
                                        for item in df.describe([.95]):
                                                if i ==5:
                                                        if math.isnan(item):
                                                                ossWriteStart95 = "-1"
                                                        else:
                                                                ossWriteStart95 = item
                                                i = i + 1 
                                        df = mdsCPU_df.loc[lmtStartTime:lmtEndTime]
                                        df = df.sum(axis=1)
                                        i = 0
                                        for item in df.describe([.95]):
                                                if i == 1:
                                                        if math.isnan(item):
                                                                mdsCPUMean = "-1"
                                                        else:
                                                                mdsCPUMean = item
                                                if i ==5:
                                                        if math.isnan(item):
                                                                mdsCPU95 = "-1"
                                                        else:
                                                                mdsCPU95 = item
                                                i = i + 1 
                                        df = mdsOPS_df.loc[lmtStartTime:lmtEndTime]
                                        df = df.sum(axis=1)

                                        i = 0
                                        for item in df.describe([.95]):
                                                if i == 1:
                                                        if math.isnan(item):
                                                                mdsOPSMean = "-1"
                                                        else:
                                                                mdsOPSMean = item
                                                if i == 3:
                                                        if math.isnan(item):
                                                                mdsOPSMin = "-1"
                                                        else:
                                                                mdsOPSMin = item
                                                if i ==5:
                                                        if math.isnan(item):
                                                                mdsOPS95 = "-1"
                                                        else:
                                                                mdsOPS95 = item
                                                i = i + 1 
                                        i = 0
                                        df = ossRead_df.loc[lmtStartTime:lmtEndTime]
                                        df.columns = [x.decode("utf-8") for x in df.columns]
                                        dfUsed = df
                                        for column in df:
                                                if df[column].mean() > ossReadLargest:
                                                        ossReadLargest = df[column].mean()
                                                        if df[column].mean() > 1:
                                                                ossReadHigher1g = ossReadHigher1g + 1
                                                        if df[column].mean() > 4:
                                                                ossReadHigher4g = ossReadHigher4g + 1
                                        df = df.sum(axis=1)
                                        numRecord = len(df.index)
                                        for item in df.describe([.95]):
                                                if i == 1:
                                                        if math.isnan(item):
                                                                ossReadMean = "-1"
                                                        else: 
                                                                ossReadMean = item
                                                if i ==5:
                                                        if math.isnan(item):
                                                                ossRead95 = "-1"
                                                        else: 
                                                                ossRead95 = item
                                                i = i + 1 
                                        i = 0
                                        df = dfUsed.loc[:, usedOSTName]
                                        #df.columns = [x.decode("utf-8") for x in df.columns]
                                        for column in df:
                                                if df[column].mean() > ossReadLargestUsed:
                                                        ossReadLargestUsed = df[column].mean()
                                        df = df.sum(axis=1)
                                        for item in df.describe([.95]):
                                                if i == 1:
                                                        if math.isnan(item):
                                                                ossReadMeanUsed = "-1"
                                                        else: 
                                                                ossReadMeanUsed = item
                                                if i ==5:
                                                        if math.isnan(item):
                                                                ossRead95Used = "-1"
                                                        else: 
                                                                ossRead95Used = item
                                                i = i + 1 
                                        i = 0
                                        df = ossWrite_df.loc[lmtStartTime:lmtEndTime]
                                        df.columns = [x.decode("utf-8") for x in df.columns]
                                        dfUsed = df
                                        for column in df:
                                                if df[column].mean() > ossWriteLargest:
                                                        ossWriteLargest = df[column].mean()
                                                        if df[column].mean() > 1:
                                                                ossWriteHigher1g = ossWriteHigher1g + 1
                                                        if df[column].mean() > 4:
                                                                ossWriteHigher4g = ossWriteHigher4g + 1
                                        df = df.sum(axis=1)
                                        #writeBytesPOSIX is originally in MB/s so change it to GB/s
                                        #because df is in Gib/s!
                                        writeBytesPct = writeBytesPOSIX/1000
                                        if df.agg('sum') != 0:
                                                writeBytesPct = (writeBytesPct * 100)/df.agg('sum')
                                        else:
                                                writeBytesPct = -1
                                        for item in df.describe([.95]):
                                                if i == 1:
                                                        if math.isnan(item):
                                                                ossWriteMean = "-1"
                                                        else:
                                                                ossWriteMean = item
                                                if i ==5:
                                                        if math.isnan(item):
                                                                ossWrite95 = "-1"
                                                        else:
                                                                ossWrite95 = item
                                                i = i + 1
                                        i = 0
                                        df = dfUsed.loc[:, usedOSTName]
                                        #df.columns = [x.decode("utf-8") for x in df.columns]
                                        for column in df:
                                                if df[column].mean() > ossWriteLargestUsed:
                                                        ossWriteLargestUsed = df[column].mean()
                                        df = df.sum(axis=1)
                                        numRecord = len(df.index)
                                        for item in df.describe([.95]):
                                                if i == 1:
                                                        if math.isnan(item):
                                                                ossWriteMeanUsed = "-1"
                                                        else:
                                                                ossWriteMeanUsed = item
                                                if i ==5:
                                                        if math.isnan(item):
                                                                ossWrite95Used = "-1"
                                                        else:
                                                                ossWrite95Used = item
                                                i = i + 1
                                        i = 0

                                except: 
                                        print("OSS computation error")
                                        pass
                        insertString = "INSERT INTO " + app_sqlite + " VALUES ('%s','%s','%s',%s,'%s','%s',%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)" %(file,
          progName, userID, jobID, startTime, endTime, int(ioStartTime), runTime, numProc, numCPU, numNode, 
          numOST, stripeSize, isknl,
          totalFile, totalIOReqPOSIX, totalMetaReqPOSIX,
          totalIOReqMPIIO, totalMetaReqMPIIO, totalIOReqSTDIO, totalMetaReqSTDIO,
          mdsCPUMean, mdsCPU95, mdsOPSMean, mdsOPSMin, mdsOPS95, ossReadMean, ossRead95, ossWriteMean, ossWrite95, 
          ossReadMeanUsed, ossRead95Used, ossWriteMeanUsed, ossWrite95Used, ossWriteLargestUsed, ossReadLargestUsed,
          ossReadHigher1g, ossReadHigher4g, ossWriteHigher1g, ossWriteHigher4g,
          totalFilePOSIX,totalFileMPIIO,totalFileSTDIO,seqWritePct,seqReadPct,consecWritePct,consecReadPct, 
          writeLess1k, writeMore1k, readLess1k, readMore1k,
          writeLess1m, writeMore1m, readLess1m, readMore1m,
          writeBytesTotal, readBytesTotal, writeRateTotal, readRateTotal,  
          writeBytesPOSIX, writeTimePOSIX, writeRatePOSIX, readBytesPOSIX, readTimePOSIX, readRatePOSIX, 
          writeBytesMPIIO,writeTimeMPIIO,writeRateMPIIO,readBytesMPIIO,readTimeMPIIO,readRateMPIIO,  
          writeBytesSTDIO,writeTimeSTDIO,writeRateSTDIO,readBytesSTDIO,readTimeSTDIO,readRateSTDIO,
          ReadReqPOSIX, WriteReqPOSIX, OpenReqPOSIX, SeekReqPOSIX, StatReqPOSIX,
          readTimePOSIXonly, writeTimePOSIXonly, readTimeMPIIOonly, writeTimeMPIIOonly, readTimeSTDIOonly, writeTimeSTDIOonly, metaTimePOSIX, metaTimeMPIIO, metaTimeSTDIO,
          slowWriteTimePOSIX, slowReadTimePOSIX, slowWriteTimeMPIIO, slowReadTimeMPIIO, IndOpenReqMPIIO, ColOpenReqMPIIO, IndWriteReqMPIIO, ColWriteReqMPIIO, IndReadReqMPIIO, ColReadReqMPIIO,
          SplitReadReqMPIIO, SplitWriteReqMPIIO, NbReadReqMPIIO, NbWriteReqMPIIO,
          OpenReqMPIIO, ReadReqMPIIO, WriteReqMPIIO,
          OpenReqSTDIO, ReadReqSTDIO, WriteReqSTDIO, SeekReqSTDIO, FlushReqSTDIO,
          ostlist, ossWriteLargest, ossReadLargest, ossWriteStart95 ,ossWriteStartLargest,writeBytesPct)
                        #print(insertString)
                        return insertString
                                                        
#main
#output = open(outputdir, 'wb+', 1)
#output = open(outputdir, 'w+', 1, 'utf-8')
output = open(outputdir, 'w+', 1, encoding='utf-8')
main_start = datetime.datetime.now()

print("At main for loop")
#scandir has MUCH better performance than os.walk
for yeardir in os.scandir(dir):
        year = int(os.path.basename(yeardir.path))
        for monthdir in os.scandir(yeardir.path):
                month = int(os.path.basename(monthdir.path))
                if month != 2:
                        continue
                for daydir in os.scandir(monthdir.path):
                        day = int(os.path.basename(daydir.path))
                        #if day != 9:
                        #       continue
                        print ("==================")
                        H5LMT_BASE = '/project/projectdirs/pma/www/daily'
                        #h5lmt_file = 'cori_snx11168.h5lmt'
                        h5lmt_file = 'cscratch'
                        lmtStartTime = datetime.datetime(year, month, day, 0,0,0 )
                        lmtEndTime = datetime.datetime(year, month, day, 23,59,59 )
                        try:
                            mdsCPU_df = tokio.tools.hdf5.get_dataframe_from_time_range(
                                    fsname=h5lmt_file,
                                    dataset_name='MDSCPUGroup/MDSCPUDataSet',
                                    #dataset_name='MDSOpsGroup/MDSOpsDataSet',
                                    datetime_start=lmtStartTime,
                                    datetime_end=lmtEndTime)
                            mdsOPS_df = tokio.tools.hdf5.get_dataframe_from_time_range(
                                    fsname=h5lmt_file,
                                    dataset_name='MDSOpsGroup/MDSOpsDataSet',
                                    datetime_start=lmtStartTime,
                                    datetime_end=lmtEndTime)
                            ossRead_df = tokio.tools.hdf5.get_dataframe_from_time_range(
                                    fsname=h5lmt_file,
                                    #dataset_name='/datatargets/readbytes',
                                    dataset_name='OSTReadGroup/OSTBulkReadDataSet',
                                    datetime_start=lmtStartTime,
                                    datetime_end=lmtEndTime) / 2.0**30
                            #/2.0**30
                            ossWrite_df = tokio.tools.hdf5.get_dataframe_from_time_range(
                                    fsname=h5lmt_file,
                                    #dataset_name='/datatargets/writebytes',
                                    dataset_name='OSTWriteGroup/OSTBulkWriteDataSet',
                                    datetime_start=lmtStartTime,
                                    datetime_end=lmtEndTime) / 2.0**30
                            print ("import Complete for: " + str(year) + " " + str(month) + " " + str(day))
                        except Exception as e:
                            mdsCPU_df = mdsOPS_df = ossRead_df = ossWrite_df = 0
                            print ("import error for: " + str(year) + " " + str(month) + " " + str(day))
                            print(e)
                        tempList = []
                        for file in os.scandir(daydir.path):
                            if file.path.endswith(".all"):
                                #jiwoo
                                #if os.path.basename(file) == 'zzren_gem_id11174226_3-25-44996-1495460099596156726_1.darshan.all' :
                                #if os.path.basename(file) == 'fgoeltl_vasp-tpc_5.4.4-knl_std_id10908761_3-12-63283-5489352668014642401_1.darshan.all' :
                                #if os.path.basename(file) == 'acmetest_theta-nlev30_id10736280_3-7-37816-16937835957214069847_1.darshan.all':
                                tempList.append(file.path)
                            """
                            elif file.path.endswith(".darshan"):
                                if not os.path.exists(file.path+".all"):
                                    print(file.path)
                                    tempList.append(file.path)
                            """
                        print ("total file count : " + str(len(tempList)))
                        #p = multiprocessing.Pool(1)
                        p = multiprocessing.Pool(32)
                        result = p.map_async(process, tempList, chunksize=1)
                        while not result.ready():
                                #print("num left: {}".format(result._number_left))
                                time.sleep(1)
                        print ("while complete")
                        try:
                            real_result = result.get()
                            p.close()
                            p.join()
                            print ("real_result len: " +str(len(real_result)))
                            for item in real_result:
                                #print(item)
                                if str(item) != "None":
                                    #print(item)
                                    #output.write((str(item) + '\n').encode('utf-8'))
                                    output.write((str(item) + '\n'))
                        except Exception as e:
                            print ("real_result error")
                            print(e)
                            timer = 0
                            while result._number_left != 0:
                                #print("num left: {}".format(result._number_left))
                                timer = timer + 1
                                time.sleep(1)
                                if timer > 1:
                                    print ("no progress, break")
                                    p.terminate()
                                    with open("skipped.txt", "a") as error:
                                        error.write("Error on :" + str(year) + " "+ str(month) + " " + str(day) + "\n")
                                    break
                        print ("Finish processing for :" + str(year) + " "+ str(month) + " " + str(day))
                        print (datetime.datetime.now() - main_start)
                        main_start = datetime.datetime.now()

print("END processing ")
print (datetime.datetime.now() - main_start)
#SQLite apply
for line in open(outputdir, 'r', 1):
        c.execute(line)

conn.commit()
conn.close()

print("END all sql execute")
