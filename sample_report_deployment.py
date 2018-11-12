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
CURRENT_SCHEMAS_QUERY = "SELECT DISTINCT schemaname FROM pg_catalog.pg_views WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY schemaname;"
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

CREATE TRIGGER "reporting$eod_trans_sum_table_trigger" BEFORE INSERT ON "reporting$eod_trans_sum_table" FOR EACH ROW EXECUTE PROCEDURE "reporting$eod_trans_sum_insert_function"();

"""

STATIC_FUNCTION_INSERT = """CREATE FUNCTION "end_of_day_transaction_summary_insert"(starttime date, endtime date) RETURNS VOID
  LANGUAGE SQL
  AS $_$
    INSERT INTO reporting$eod_trans_sum_table
    ( insert_date,start_date,end_date,operation,total_count,currency,total_amount,total_fees,total_discounts,total_promotions,total_coupons,additional_info )
    (
      select
        now()::date                                                     as insert_date,
        starttime                                                       as start_date,
        endtime                                                         as end_date,
        operationtype                                                   as operation,
        count(*)                                                        as total_count,
        currencycode                                                    as currency,
        sum(coalesce(amount, 0.0))                                      as total_amount,
        sum(coalesce(fromexternalfee, 0.0))
                    + sum(coalesce(frominternalfee, 0.0))
                    + sum(coalesce(toexternalfee, 0.0))
                    + sum(coalesce(tointernalfee, 0.0))                 as total_fees,
        sum(coalesce(fromdiscount, 0.0))
                    + sum(coalesce(todiscount, 0.0))                    as total_discounts,
        sum(coalesce(frompromotion, 0.0))
                    + sum(coalesce(topromotion, 0.0))                   as total_promotions,
        null::numeric                                                   as total_coupons,
        toserviceprovider                                               as additional_info
      from "global".committed_financialevent(starttime, endtime)
      group by operationtype, currencycode, toserviceprovider
      order by operationtype, currencycode, toserviceprovider
    );
  $_$;
"""

STATIC_NEW_EOD_FUNCTION_GENERAL = """ RETURNS TABLE(start_date date, end_date date, operation text, total_count numeric, currency text, total_amount numeric, total_fees numeric, total_discounts numeric, total_promotions numeric, total_coupons numeric, additional_info text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  select
    starttime                                                       as start_date,
    endtime                                                         as end_date,
    operation                                                       as operation,
    sum(total_count)                                                as total_count,
    currency                                                        as currency,
    sum(coalesce(total_amount, 0.0))                                as total_amount,
    sum(coalesce(total_fees, 0.0))                                  as total_fees,
    sum(coalesce(total_discounts, 0.0))                             as total_discounts,
    sum(coalesce(total_promotions, 0.0))                            as total_promotions,
    null::numeric                                                   as total_coupons,
    additional_info                                                 as additional_info
  from "RDS".reporting$eod_trans_sum_table
  where end_date BETWEEN starttime AND endtime
  group by operation, currency, additional_info
$function$
"""

STATIC_GRANTS_FOR_NEW_OBJECTS = """ALTER TABLE "reporting$eod_trans_sum_table" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p0" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p1" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p2" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p3" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p4" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p5" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p6" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p7" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p8" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p9" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p10" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p11" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p12" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p13" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p14" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p15" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p16" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p17" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p18" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p19" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p20" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p21" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p22" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p23" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p24" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p25" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p26" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p27" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p28" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p29" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p30" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p31" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p32" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p33" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p34" OWNER TO "RDS";
ALTER TABLE "reporting$eod_trans_sum_table_p35" OWNER TO "RDS";

ALTER FUNCTION "RDS"."end_of_day_transaction_summary_insert"(date,date) OWNER TO postgres;
ALTER FUNCTION "Stanbic"."example$end_of_day_transaction_summary"(date,date) OWNER TO postgres;
ALTER FUNCTION "global"."example$end_of_day_transaction_summary"(date,date) OWNER TO postgres;
GRANT ALL ON FUNCTION "Stanbic"."example$end_of_day_transaction_summary"(date,date) TO "RDS";
GRANT ALL ON FUNCTION "global"."example$end_of_day_transaction_summary"(date,date) TO "RDS";
GRANT ALL ON FUNCTION "RDS"."end_of_day_transaction_summary_insert"(date,date) TO "RDS";
"""

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
    cursorLenght = 0
    resultQuery = None
    resultQueryClean = []

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
            self.resultQuery = self.internalCursor.fetchall()

            # - Codigo Limpia Shit
            for i in self.resultQuery:
                if type(i[0]) == unicode:
                    self.resultQueryClean.append( i[0].encode('ascii') )

            self.cursorLenght = len(self.resultQuery) 
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

    def stackPartition(self,table):
        tempTuple , = self.partition
        self.table = table + str(tempTuple[0])
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
    schemasList = [] 
    schemasQueries = []

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

    def buildShemaQueries(self):
        # - Build queries for schemas
        for i in self.schemasList:
            self.schemasQueries.append( 'ALTER FUNCTION "%s"."example$end_of_day_transaction_summary"(date, date) RENAME TO example$end_of_day_transaction_summary_orig;' % ( i ) + '\n' )
            self.schemasQueries.append( 'CREATE OR REPLACE FUNCTION "%s"."example$end_of_day_transaction_summary"(starttime date, endtime date)' % ( i ) + '\n' + STATIC_NEW_EOD_FUNCTION_GENERAL )

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
    pb1.partition = db1.resultQuery
    pb1.stackPartition(TABLE_NAME_EOD_SUM_REPORT)
    pb1.addTuple()

    # - Generate tuples in the list
    qb1 = QueryBuilder()
    qb1.addTuple( pb1.partitionTuple )

    # - Generate child tables detail
    qb1.generateChildTableQuery()

    # - Get schemas to generate backups & procedures
    db1.queryRDS(CURRENT_SCHEMAS_QUERY)
    qb1.schemasList = db1.resultQueryClean
    qb1.buildShemaQueries()

fhb1 = FileHandlerBox()
fhb1.touchOrOpenMyFile()
fhb1.currentFile.write( STATIC_TABLE_PART_EOD_SUMM_1 )

for i in qb1.childTablesList:
    fhb1.currentFile.write( i + '\n' )

fhb1.currentFile.write( '\n' )
fhb1.currentFile.write( STATIC_TABLE_PART_EOD_SUMM_2 )

for i in qb1.functionProcedureList:
  fhb1.currentFile.write( i + '\n' )

fhb1.currentFile.write( STATIC_TABLE_PART_EOD_SUMM_3 )

fhb2 = FileHandlerBox()
fhb2.touchOrOpenMyFile()

for i in qb1.schemasQueries:
    fhb2.currentFile.write( i + '\n' )

fhb2.currentFile.write( STATIC_GRANTS_FOR_NEW_OBJECTS )

fhb1.closeMyFile()
fhb2.closeMyFile()
