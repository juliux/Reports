#!/usr/bin/env python
# -*- coding: utf-8 -*-
# +-+-+-+-+ +-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+
# |B|A|S|E| |6|4| |E|N|C|O|D|E|R| |D|E|C|O|D|E|R|
# +-+-+-+-+ +-+-+ +-+-+-+-+-+-+-+ +-+-+-+-+-+-+-+

import base64
import sys

# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+
# |C|L|A|S|S| |D|E|F|I|N|I|T|I|O|N|
# +-+-+-+-+-+ +-+-+-+-+-+-+-+-+-+-+

class FileBox:
    inputFileName = ""
    outputFileName = ""
    stream64 = None
    finalStream64 = None
    currentFile =  None

    def __init__( self, inputFile, outputFile ):
        self.inputFileName = inputFile
        self.outputFileName = outputFile

    def encodeFile( self ):
        with open( self.inputFileName, "rb" ) as archivo:
            self.stream64 = archivo.read()
            self.finalStream64 = base64.b64encode(self.stream64)

    def decodeFile( self ):
        #with open( self.inputFileName, "rb" ) as archivo:
        #    self.stream64 = archivo.read()
        #    self.finalStream64 = base64.decode(self.stream64)
        base64.decode( open( self.inputFileName, 'rb' ), open( self.outputFileName, 'wb+'))

    def touchFinalFile( self ):
        self.currentFile = open(self.outputFileName, "wb+" )
        self.currentFile.write( self.finalStream64 )

    def closeMyFile( self ):
        self.currentFile.close()

# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+
# |D|E|P|L|O|Y|M|E|N|T| |L|O|G|I|C|
# +-+-+-+-+-+-+-+-+-+-+ +-+-+-+-+-+

#print sys.getdefaultencoding()
if len(sys.argv) == 4:
    f1 = FileBox( sys.argv[1], sys.argv[2] ) 
    if str( sys.argv[3] ) == 'encode':
        try:
            f1.encodeFile()
        except:
            print "Failing encoding"

        try:
            f1.touchFinalFile()
            f1.closeMyFile()
        except:
            print "Error creating the file"

    if str( sys.argv[3] ) == 'decode':
        try:
            f1.decodeFile()
            base64.decode(open(sys.argv[1], 'rb'), open(sys.argv[2], 'wb'))
        except:
            print "Error decoding the file"
else:
    print "Use SCRIPT inFile outFile encode or decode. Example: ./encoder_base64.py inFile outFile encode"
    sys.exit(0)

#print "This is the name of the script: ", sys.argv[0]
#print "Number of arguments: ", len(sys.argv)
#print "The arguments are: " , str(sys.argv)
