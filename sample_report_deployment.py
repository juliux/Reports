#!/usr/bin/env python

# +-+-+-+-+-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |S|A|M|P|L|E| |R|E|P|O|R|T|S| |D|E|P|L|O|Y|M|E|N|T|
# +-+-+-+-+-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

import os
import sys
from termcolor import colored
from datetime import datetime, timedelta
import pg8000
import calendar
import logging
import string
from random import *

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

# - Banner generation

STATIC_BANNER = "    S A M P LE   R E P O R T   D E P L O Y M E N T   T O O L"
STATIC_LINE = "+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+"
EMPTY_SPACE = ""

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

# - Partition stacking

CURRENT_PARTITION_QUERY = "SELECT a.currentpart FROM rds$datepart_data a INNER JOIN rds$datepartitioning b on b.id = a.id WHERE b.tablename = 'report$reservation';"
TABLE_NAME_EOD_SUM_REPORT = "reporting$eod_trans_sum_table_p"

# - EOD Table stacking

STATIC_TABLE_PART_EOD_SUMM_1 = """CREATE TABLE "reporting$eod_trans_sum_table" (
  id SERIAL,
  insert_date DATE NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  operation character varying(32) NOT NULL,
  total_count bigint,
  currency currencycode,
  total_amount amount,
  total_fees amount,
  total_discounts amount,
  total_promotions amount,
  total_coupons bigint,
  additional_info text
);

"""

STATIC_TABLE_PART_EOD_SUMM_2 = """CREATE FUNCTION "reporting$eod_trans_sum_insert_function"() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
BEGIN
"""

STATIC_TABLE_PART_EOD_SUMM_3 = """  ELSE
    RAISE EXCEPTION 'out of range: %. Fix function reporting$eod_trans_sum_insert_function()', NEW.end_date;
  END IF;
  RETURN NULL;
END;
$_$;

CREATE TRIGGER "reporting$eod_trans_sum_table_trigger" BEFORE INSERT ON "reporting$eod_trans_sum_table" FOR EACH ROW EXECUTE PROCEDURE "reporting$eod_trans_sum_insert_function"();"""

# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |C|L|A|S|S| |D|E|F|I|N|I|T|I|O|N|
# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

class OsAgent:
 
    commando = 'clear'
    token = 'ABc123456'
   
    def __init__(self, commando, token):
        self.commando = commando
        self.token = token

    def executeCall(self):
        os.system(self.commando)

    def readKeyboard(self, question_string):
        self.token = raw_input( question_string )
   
    #def exportVar(self,variable):
     #   os.environ['"' + variable + '"'] = self.token

    #@classmethod
    @staticmethod
    def clearScreen():
        os.system('clear')

    @staticmethod
    def elegantExit():
        sys.exit()

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class DBbox:
    
    user = "RDS"
    passwd = "ABc123456"
    database = "RDS"
    port = "5432"
    conexion = None
    internalCursor = None

    def __init__(self,user,passwd,database,port):
        self.user = user
        self.passwd = passwd
        self.database = database
        self.port = port

    def openConnection(self, cls):
        try:
            self.conexion = pg8000.connect( user = self.user, database = self.database, password = self.passwd, port = self.port )
        except Exception as err:
            #print(err)
            cls.exception("Exception occurred connecting to the database")
        
    def queryRDS(self,query):
        try:
            self.internalCursor = self.conexion.cursor()
            self.internalCursor.execute(query)
        except Exception as err:
            print(err)

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class PartitionBox:

    partition = None
    data_rango = tuple()
    partitionTuple = None
    inicio =""
    fin = ""
    table = ""
    FORMAT_STRING = "%Y-%m-%d"
    

    def monthRange(self,year,month):
        self.date_rango = calendar.monthrange(year,month)
        self.inicio, self.fin = self.date_rango
        self.inicio = year + "-" + month + "-" + self.inicio
        self.fin = year + "-" + month + "-" + self.fin

    def partitionNumber(self,partition):
        pass        

    def stackPartition(self,table):
        self.table = table + str(self.partition[0])
        self.inicio = datetime.today().replace(day=1).strftime('%Y-%m-%d')
        self.data_rango = calendar.monthrange( datetime.now().year, datetime.now().month )
        self.fin = str(datetime.now().year) + '-' + str(datetime.now().month) + '-' + str(self.data_rango[1])
        x = datetime.strptime(self.fin,self.FORMAT_STRING)
        self.fin =  x + timedelta(days=1)
        self.fin = self.fin.strftime('%Y-%m-%d')

    def addTuple(self):
        self.partitionTuple = ( self.table, self.inicio, self.fin ) 

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class QueryBuilder:
    
    finalQueryList = []
    childTablesList = []
    functionProcedureList = []
    FORMAT_STRING = "%Y-%m-%d" 

    def addTuple(self,myTuple):
        self.finalQueryList.append(myTuple) 
        # - Tuple Generation
        table, inicio, fin = myTuple
        currentPartition = table[31:32]
        currentTable = table[0:31]
        iteration = int(table[31:32]) + 1
        while currentPartition != iteration: 
            # - Time Control
            newTupleElementFin = inicio
            tempTuple = datetime.strptime(inicio, self.FORMAT_STRING) - timedelta(days=1)
            newTupleElementInicio = tempTuple.replace(day=1)
            newTupleElementInicio = newTupleElementInicio.strftime('%Y-%m-%d')
            # - Table Control
            if currentPartition == 0:
               currentPartition = 35
            else:
               currentPartition = int(currentPartition) -1
        
            newTupleElementTable = currentTable + str(currentPartition)
            finalTempTuple = ( newTupleElementTable, newTupleElementInicio, newTupleElementFin )
            self.finalQueryList.append(finalTempTuple)
            table, inicio, fin = finalTempTuple
    
    def generateChildTableQuery(self):
        # - Tuple Processing
        firstOne = True
        for processTuple in self.finalQueryList:
            table, inicio, fin = processTuple
            #tempString = 'CREATE TABLE "%s" ( CONSTRAINT "%s_check" CHECK (((end_date >= \'' % ( table, table ) #+ inicio + '' ) AND (end_date < '' + fin + '' )))) INHERITS ("reporting$eod_trans_sum_table");'
            self.childTablesList.append('CREATE TABLE "%s" ( CONSTRAINT "%s_check" CHECK (((end_date >= \'%s\' ) AND (end_date < \'%s\' )))) INHERITS ("reporting$eod_trans_sum_table");' % ( table, table, inicio, fin ) )
            #self.childTablesList.append(tempString)
            if firstOne:
                self.functionProcedureList.append('  IF ( NEW.end_date >= \'%s\' AND NEW.end_date < \'%s\' ) THEN INSERT INTO %s VALUES (NEW.*);' % ( inicio, fin, table ) )
                firstOne = False
            else:
                self.functionProcedureList.append('  ELSEIF ( NEW.end_date >= \'%s\' AND NEW.end_date < \'%s\' ) THEN INSERT INTO %s VALUES (NEW.*);' % ( inicio, fin, table ) )

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class FileHandlerBox:

    FILE_NAME = "query."
    FILE_PERMISSION = 'w+'
    minMax = (8,12)
    #allchar = string.ascii_letters + string.punctuation + string.digits
    allchar = string.ascii_letters + string.digits
    currentQueryFileName = ""
    currentFile = None

    def touchOrOpenMyFile(self):
        min_char, max_char = self.minMax
        randomExtension = "".join(choice(self.allchar) for x in range(randint(min_char, max_char)))
        self.currentQueryFileName = self.FILE_NAME + randomExtension + '.sql'
        self.currentFile = open(self.currentQueryFileName, self.FILE_PERMISSION)   

    def closeMyFile(self):
        self.currentFile.close()

# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+
# |D|E|P|L|O|Y|M|E|N|T| |L|O|G|I|C|
# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+

# - Setup logger

botLogger = logging.getLogger(__name__)
f_handler = logging.FileHandler('sample_report.log')
f_handler.setLevel(logging.ERROR)
f_format = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
f_handler.setFormatter(f_format)
botLogger.addHandler(f_handler)

# - Main banner printing

OsAgent.clearScreen()
print colored( STATIC_LINE, 'yellow') 
print colored( STATIC_BANNER, 'white')
print colored( STATIC_LINE, 'yellow') 
print( EMPTY_SPACE )

# - Read RDS password

os1 = OsAgent('clear','ABc12345')
os1.readKeyboard("Enter RDS password: ")

# - Open connection

db1 = DBbox('RDS',os1.token,'RDS',5432)
db1.openConnection(botLogger)

if db1.conexion is None:
    botLogger.error("Exiting because connection None, Elegant Exit Called")
    os1.elegantExit()
else:
# - Getting last partition
    db1.queryRDS(CURRENT_PARTITION_QUERY)
    pb1 = PartitionBox()
    pb1.partition = db1.internalCursor.fetchone()
    pb1.stackPartition(TABLE_NAME_EOD_SUM_REPORT)
    pb1.addTuple()

#print(pb1.partition)
#print(pb1.table)
#print(pb1.inicio)
#print(pb1.data_rango)
#print(pb1.fin)

#print( pb1.partitionTuple )

qb1 = QueryBuilder()
qb1.addTuple( pb1.partitionTuple )
qb1.generateChildTableQuery()

#print( qb1.finalQueryList )
#print( qb1.childTablesList )
#print( qb1.functionProcedureList )

#print(help(pg8000))a


fhb1 = FileHandlerBox()

fhb1.touchOrOpenMyFile()

#print( fhb1.currentQueryFileName )
#print( fhb1.currentFile )

#print( qb1.finalQueryList )
#print( qb1.childTablesList )
#print( qb1.functionProcedureList )

fhb1.currentFile.write( STATIC_TABLE_PART_EOD_SUMM_1 )

for i in qb1.childTablesList:
    fhb1.currentFile.write( i + '\n' )

fhb1.currentFile.write( '\n' )
fhb1.currentFile.write( STATIC_TABLE_PART_EOD_SUMM_2 )

for i in qb1.functionProcedureList:
  fhb1.currentFile.write( i + '\n' )

fhb1.currentFile.write( STATIC_TABLE_PART_EOD_SUMM_3 )

fhb1.closeMyFile()
