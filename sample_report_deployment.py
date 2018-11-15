#!/usr/bin/env python

# +-+-+-+-+-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |S|A|M|P|L|E| |R|E|P|O|R|T|S| |D|E|P|L|O|Y|M|E|N|T|
# +-+-+-+-+-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

from termcolor import colored
from datetime import datetime, timedelta
from progress.bar import Bar
from random import *
import pg8000
import sys
import os
import calendar
import logging
import string
import time

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

# - Banner generation

STATIC_BANNER = "    S A M P L E   R E P O R T   D E P L O Y M E N T   T O O L"
STATIC_LINE = "+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+"
STATIC_DOTTED_LINE = "................................................................."
EMPTY_SPACE = ""
STATIC_BANNER_SEARCHING_DATA = "Searching stored data in report$reservation:"
STATIC_NON_INTERACTIVE_BANNER = "Entering non interactive mode."
STATIC_ERROR_IN_SYNTAX = "Error in syntax, dropping session & calling ellegant exit."
STATIC_USAGE_BANNER = """
Usage:
./sample_report_deployment.py nonInteractive RDS_User_Password Function

Or Version detail:
./sample_report_deployment.py version

Example:
./sample_report_deployment.py nonInteractive ABc123456 generateEODReport

Functions are: generateEODReport | agentHierarchyReport | serviceProviderTransReport | generateEODReportRollback | agentHierarchyReportRollback | serviceProviderTransReportRollback
"""
STATIC_MENU_BANNER = """Select your report:

[1] End of day summary report.
[2] Agent hierarchy transaction summary report.
[3] Service provider transaction summary report.
[4] End of day summary report - ROLLBACK.
[5] Agent hierarchy transaction summary report - ROLLBACK.
[6] Service provider transaction summary report - ROLLBACK.
................................................................."""

STATIC_ALERT = "    H A V E   T O   B E  1, 2, 3, 4, 5  OR  6 !"

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
$function$;
"""

STATIC_GRANTS_FOR_NEW_OBJECTS_1 = """ALTER TABLE "reporting$eod_trans_sum_table" OWNER TO "RDS";
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
"""

STATIC_GRANTS_FOR_NEW_OBJECTS_2 = """ALTER FUNCTION "RDS"."end_of_day_transaction_summary_insert"(date,date) OWNER TO postgres;
ALTER FUNCTION "Stanbic"."example$end_of_day_transaction_summary"(date,date) OWNER TO postgres;
ALTER FUNCTION "global"."example$end_of_day_transaction_summary"(date,date) OWNER TO postgres;
GRANT ALL ON FUNCTION "Stanbic"."example$end_of_day_transaction_summary"(date,date) TO "RDS";
GRANT ALL ON FUNCTION "global"."example$end_of_day_transaction_summary"(date,date) TO "RDS";
GRANT ALL ON FUNCTION "RDS"."end_of_day_transaction_summary_insert"(date,date) TO "RDS";
"""

STATIC_EOD_ROLLBACK_OBJECT_1 = """DROP TABLE reporting$eod_trans_sum_table CASCADE;
DROP FUNCTION reporting$eod_trans_sum_insert_function();
DROP FUNCTION end_of_day_transaction_summary_insert(date,date);

"""
# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |C|L|A|S|S| |D|E|F|I|N|I|T|I|O|N|
# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

class OsAgent:
 
    commando = 'clear'
    token = 'ABc123456'
    myProgressBar = None
    parametersList = []
    state = ""
    whichReport = ""
    myselfScript = ""
    rawKeyboard = ""
    validReports = ['generateEODReport','agentHierarchyReport','serviceProviderTransReport','generateEODReportRollback','agentHierarchyReportRollback','serviceProviderTransReportRollback']
   
    def __init__(self, commando, token):
        self.commando = commando
        self.token = token

    def executeCall(self):
        os.system(self.commando)

    def readKeyboard(self, question_string):
        self.rawKeyboard = raw_input( question_string )
   
    #def exportVar(self,variable):
     #   os.environ['"' + variable + '"'] = self.token

    #@classmethod
    @staticmethod
    def clearScreen():
        os.system('clear')

    @staticmethod
    def elegantExit():
        sys.exit()

    @staticmethod
    def waitPlease(mySeconds):
        time.sleep(mySeconds)

    def progressBarPrint(self, message, listLen):
        self.myProgressBar = Bar(message, max=listLen)

    def controlInteractive(self):
        self.myselfScript, self.state, self.token, self.whichReport = self.parametersList

    def mainMenu(self):
        print colored(STATIC_MENU_BANNER, 'yellow')

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
    finalDatesDataPopulation = []

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

    def generaDateQueries(self, myDateList):
        for i in myDateList:
            tempStartUp, tempEndUp = i
            tempDateValue = datetime.strptime( tempStartUp, self.FORMAT_STRING )
            first_weekday, month_days = calendar.monthrange(tempDateValue.year, tempDateValue.month)
            for mday in xrange(1, month_days + 1):
                myFinalDateObject = datetime(year=tempDateValue.year, month=tempDateValue.month, day=mday )
                myFinalDateString = myFinalDateObject.strftime('%Y-%m-%d')
                self.finalDatesDataPopulation.append( myFinalDateString )

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class QueryBuilder:
    
    finalQueryList = []
    childTablesList = []
    functionProcedureList = []
    FORMAT_STRING = "%Y-%m-%d"
    schemasList = [] 
    schemasQueries = []
    schemasQueriesRollback = []
    monthWithDataQueries = []
    dataFoundDates = []
    finalDataInsertQueries = []

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

    def buildShemaQueriesRollback(self):
        # - Build queries for Rollbacks using schemas
        for i in self.schemasList:
            #self.schemasQueriesRollback.append( 'DROP FUNCTION "%s".example$end_of_day_transaction_summary(date,date);' % ( i ) + '\n' )
            self.schemasQueriesRollback.append( 'DROP FUNCTION "%s".example$end_of_day_transaction_summary(date,date);' % ( i ) )
            #self.schemasQueriesRollback.append( 'ALTER FUNCTION "%s"."example$end_of_day_transaction_summary_orig"(date, date) RENAME TO example$end_of_day_transaction_summary;' % ( i ) + '\n' )
            self.schemasQueriesRollback.append( 'ALTER FUNCTION "%s"."example$end_of_day_transaction_summary_orig"(date, date) RENAME TO example$end_of_day_transaction_summary;' % ( i ) )

    def buildAggregationTableQueries(self):
        for i in qb1.finalQueryList:
            table, startDate, endDate = i
            tempString = 'SELECT count(*) FROM report$reservation WHERE finalizedtime BETWEEN \'%s\' AND \'%s\';' % ( startDate, endDate )
            myTempTuple = ( startDate, endDate, tempString )
            self.monthWithDataQueries.append( myTempTuple )

    def generaDataQueries(self, myDateList):
        for i in myDateList:
            myTempQuery = 'SELECT "RDS".end_of_day_transaction_summary_insert(\'%s\',\'%s\');' % ( i, i )
            self.finalDataInsertQueries.append( myTempQuery )

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

# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class FinalReport:

    myVersion = "Current Version is : 1.0 - support for EOD report Only"

    def generateEODReport(self,db1,pb1,qb1,fhb1,fhb2,fhb3):
        # - Open connection
        db1 = DBbox('RDS',db1.passwd,'RDS',5432)
        db1.openConnection(botLogger)
        print colored( 'Open DB connection.', 'white')
        print colored( STATIC_DOTTED_LINE , 'yellow')
        print( EMPTY_SPACE )

        if db1.conexion is None:
            botLogger.error("Exiting because connection None, Elegant Exit Called")
            print colored( 'Exiting because connection None, Elegant Exit Called.', 'red')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            os1.elegantExit()
        else:
            # - End of day report    
            # - Getting last partition
            print colored( 'Getting current partition.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            db1.queryRDS(CURRENT_PARTITION_QUERY)
            pb1 = PartitionBox()
            pb1.partition = db1.resultQuery
            pb1.stackPartition(TABLE_NAME_EOD_SUM_REPORT)
            pb1.addTuple()

            # - Generate tuples in the list
            print colored( 'Generating tuples for queries.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            qb1 = QueryBuilder()
            qb1.addTuple( pb1.partitionTuple )

            # - Generate child tables detail
            print colored( 'Generating Child Tables queries.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            qb1.generateChildTableQuery()

            # - Get schemas to generate backups & procedures
            print colored( 'Building other schema quieres.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            db1.queryRDS(CURRENT_SCHEMAS_QUERY)
            qb1.schemasList = db1.resultQueryClean
            qb1.buildShemaQueries()

            # - Generate queries for aggregated tables.
            #qb1.finalQueryList
            qb1.buildAggregationTableQueries()
            print colored( STATIC_BANNER_SEARCHING_DATA , 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            os1.progressBarPrint( 'Searching Data Per Month:', len(qb1.monthWithDataQueries) )
            for i in qb1.monthWithDataQueries:
                tempStartDate , tempEndDate, tempQuery = i
                db1.queryRDS( tempQuery )
                if db1.resultQuery[0][0] != 0:
                    tempTupleGenerated = ( tempStartDate, tempEndDate )
                    qb1.dataFoundDates.append( tempTupleGenerated )
                os1.myProgressBar.next()

            print( EMPTY_SPACE )
            os1.myProgressBar.finish()
            pb1.generaDateQueries(qb1.dataFoundDates)
            qb1.generaDataQueries(pb1.finalDatesDataPopulation)

            print colored( 'Dumping files with the queries.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )

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
            fhb1.currentFile.write( STATIC_FUNCTION_INSERT )
            fhb1.currentFile.write( STATIC_GRANTS_FOR_NEW_OBJECTS_1 )

            fhb2 = FileHandlerBox()
            fhb2.touchOrOpenMyFile()

            for i in qb1.schemasQueries:
                fhb2.currentFile.write( i + '\n' )

            fhb2.currentFile.write( STATIC_GRANTS_FOR_NEW_OBJECTS_2 )

            fhb3 = FileHandlerBox()
            fhb3.touchOrOpenMyFile()

            for i in qb1.finalDataInsertQueries:
                fhb3.currentFile.write( i + '\n' )

            fhb1.closeMyFile()
            fhb2.closeMyFile()
            fhb3.closeMyFile()

            os.rename( fhb1.currentQueryFileName, 'query.RDS.sql' )
            os.rename( fhb2.currentQueryFileName, 'query.postgres.sql' )
            os.rename( fhb3.currentQueryFileName, 'query.inserts.sql' )

    def generateEODReportRollback(self,db1,fhb4,fhb5):
        # - Open connection
        db1 = DBbox('RDS',db1.passwd,'RDS',5432)
        db1.openConnection(botLogger)
        print( EMPTY_SPACE )
        print colored( 'Open DB connection.', 'white')
        print colored( STATIC_DOTTED_LINE , 'yellow')
        print( EMPTY_SPACE )

        if db1.conexion is None:
            botLogger.error("Exiting because connection None, Elegant Exit Called")
            print colored( 'Exiting because connection None, Elegant Exit Called.', 'red')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            os1.elegantExit()
        else:
            # - End of day report rollback   
            # - Get schemas to generate rollback queries
            print colored( 'Building rollback schema queries.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )
            db1.queryRDS(CURRENT_SCHEMAS_QUERY)
            qb1.schemasList = db1.resultQueryClean
            qb1.buildShemaQueriesRollback()

            print colored( 'Dumping files with the queries.', 'white')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print( EMPTY_SPACE )

            fhb4 = FileHandlerBox()
            fhb4.touchOrOpenMyFile()
            fhb5 = FileHandlerBox()
            fhb5.touchOrOpenMyFile()

            fhb4.currentFile.write( STATIC_EOD_ROLLBACK_OBJECT_1 )

            for i in qb1.schemasQueriesRollback:
                fhb5.currentFile.write( i + '\n' )

            fhb4.closeMyFile()
            fhb5.closeMyFile()

            os.rename( fhb4.currentQueryFileName, 'query.RDS.Rollback.sql' )
            os.rename( fhb5.currentQueryFileName, 'query.postgres.Rollback.sql' )

    def agentHierarchyReport(self):
        pass

    def serviceProviderTransReport(self):
        pass

    @staticmethod
    def dropTrashUsage():
        # - Entering error in syntax
        print colored(STATIC_ERROR_IN_SYNTAX, 'red')
        print colored( STATIC_DOTTED_LINE, 'yellow')
        print( EMPTY_SPACE )
        print colored(STATIC_USAGE_BANNER, 'yellow')
        print colored('Just ./sample_report_deployment.py without parameters for interactive session!!!', 'green')
        print( EMPTY_SPACE )
        print colored( STATIC_DOTTED_LINE, 'yellow')

    @staticmethod
    def printMyVersion():
        OsAgent.clearScreen()
        print colored( STATIC_DOTTED_LINE, 'yellow')
        print colored(FinalReport.myVersion, 'green')
        print colored( STATIC_DOTTED_LINE, 'yellow')


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

os1 = OsAgent('clear','ABc12345')
os1.parametersList = sys.argv

if len(os1.parametersList) != 1:
    print colored( STATIC_NON_INTERACTIVE_BANNER, 'white')
    print colored( STATIC_DOTTED_LINE, 'yellow')
    print( EMPTY_SPACE )
    if len( os1.parametersList ) == 4:
        if os1.parametersList[1] == 'nonInteractive' and os1.parametersList[3] in os1.validReports:
            # - Entering non interactive
            os1.controlInteractive()
            # - will go interactive
            # - Read RDS password
            # - Open connection
            db1 = DBbox('RDS',os1.token,'RDS',5432)
            pb1 = PartitionBox()
            qb1 = QueryBuilder()
            fhb1 = FileHandlerBox()
            fhb2 = FileHandlerBox()
            fhb3 = FileHandlerBox()
            fhb4 = FileHandlerBox()
            fhb5 = FileHandlerBox()
            myReportBox = FinalReport()
                
            if os1.whichReport == 'generateEODReport':
                # - EOD report
                myReportBox.generateEODReport(db1,pb1,qb1,fhb1,fhb2,fhb3)
            elif os1.whichReport == 'agentHierarchyReport':
                # - Agent Hierarchy report
                print( 'Agent Hierarchy report' )
            elif os1.whichReport == 'serviceProviderTransReport':
                # - Service Provider report
                print( 'Service Provider report' )
            elif os1.whichReport == 'generateEODReportRollback':
                # - Service Provider report
                #print( 'EOD report Rollback' )
                myReportBox.generateEODReportRollback(db1,fhb4,fhb5)
            elif os1.whichReport == 'agentHierarchyReportRollback':
                # - Service Provider report
                print( 'Agent Hierarchy report Rollback' )
            elif os1.whichReport == 'serviceProviderTransReportRollback':
                # - Service Provider report
                print( 'Service Provider report Rollback' )
        else:
            # - Entering error in syntax
            FinalReport.dropTrashUsage()
    elif len(os1.parametersList) == 2:
        if os1.parametersList[1] == 'version':
            FinalReport.printMyVersion()
            OsAgent.elegantExit()
        else:
            # - Print trashy help
            FinalReport.dropTrashUsage()         
    else:
        # - Entering incorrect parameters
        FinalReport.dropTrashUsage()
else:
    # - will go interactive
    # - Read RDS password
    os1.readKeyboard("Enter RDS password: ")
    print colored( STATIC_DOTTED_LINE , 'yellow')
    print( EMPTY_SPACE )

    db1 = DBbox('RDS',os1.rawKeyboard,'RDS',5432)
    pb1 = PartitionBox()
    qb1 = QueryBuilder()
    fhb1 = FileHandlerBox()
    fhb2 = FileHandlerBox()
    fhb3 = FileHandlerBox()
    fhb4 = FileHandlerBox()
    fhb5 = FileHandlerBox()
    myReportBox = FinalReport()

    tempFlag = True
    while tempFlag :
        OsAgent.clearScreen()
        print colored( STATIC_LINE, 'yellow')
        print colored( STATIC_BANNER, 'white')
        print colored( STATIC_LINE, 'yellow')
        print( EMPTY_SPACE )
        os1.mainMenu()
        os1.readKeyboard('Type 1 to 6 options: ')
        if os1.rawKeyboard == '1' or os1.rawKeyboard == '2' or os1.rawKeyboard == '3' or os1.rawKeyboard == '4' or os1.rawKeyboard == '5' or os1.rawKeyboard == '6':
            tempFlag = False
        else:
            OsAgent.clearScreen()
            print colored( STATIC_DOTTED_LINE , 'yellow')
            print colored( STATIC_ALERT,'yellow')
            print colored( STATIC_DOTTED_LINE , 'yellow')
            os1.waitPlease(1)

    if os1.rawKeyboard == '1':
        # - EOD Report
        myReportBox.generateEODReport(db1,pb1,qb1,fhb1,fhb2,fhb3)
    elif os1.rawKeyboard == '2':
        # - Agent Hierarchy report
        print( 'Agent Hierarchy report' )
    elif os1.rawKeyboard == '3':
        # - Service Provider report
        print( 'Service Provider report' )
    elif os1.rawKeyboard == '4':
        # - EOD Report Rollback
        #print( 'EOD Report Rollback' )
        myReportBox.generateEODReportRollback(db1,fhb4,fhb5)
    elif os1.rawKeyboard == '5':
        # - agentHierarchyReport Rollback
        print( 'Agent Hierarchy report Rollback' )
    elif os1.rawKeyboard == '6':
        # - Service Provider report Rollback
        print( 'Service Provider report Rollback' )

