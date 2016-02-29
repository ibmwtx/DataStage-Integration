//***************************************************************************
// (c) Copyright IBM Corp. 2013 All rights reserved.
// 
// The following sample of source code ("JDBCStage") is owned by International 
// Business Machines Corporation or one of its subsidiaries ("IBM") and is 
// copyrighted and licensed, not sold. You may use, copy, modify, and 
// distribute the Sample in any form without payment to IBM, for the purpose of 
// assisting you in the development of your applications.
// 
// The Sample code is provided to you on an "AS IS" basis, without warranty of 
// any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR 
// IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do 
// not allow for the exclusion or limitation of implied warranties, so the above 
// limitations or exclusions may not apply to you. IBM shall not be liable for 
// any damages you suffer as a result of using, copying, modifying or 
// distributing the Sample, even if IBM has been advised of the possibility of 
// such damages.
//***************************************************************************

package com.ibm.is.cc.javastage.wtx;

import com.ibm.is.cc.javastage.api.*;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import java.io.*;

/*******************************************************************************

This class provides a means to invoke WebSphere TX maps from DataStage jobs.
It maps input and output links to input and output cards of TX maps.

It supports the following stage properties:

MapFile         : The full path for the TX map file.
WorkDir         : The path of the work directory.
ResourceFile    : The path of the resource file.
RunEach         : Specifies whether to run the map at the end of the wave, or to run 
                  the map for each input row.  Valid values are 'Wave' and 'Row'.
MapTrace        : Turn map trace on or off.  Valid values are 'On' and 'Off'                  
MapAudit        : Turn map audit on or off.  Valid values are 'On' and 'Off'                  
FailOnWarning   : Fail the job if the map returns a warning.
Debug           : If set to 'true' additional log messages are produced.
TraceFile       : Specifies the name of a trace file to which trace information 
                  will be written.

It supports the following link properties:

Card            : The number of the card in the map (offset from 1).
Charset         : The character set for string data.  If not set the platform 
                  default is used.
UseStrings      : If 'true' all columns will be transferred as strings.
Delimiter       : The column delimiter.
Location        : The location of the column delimiter.  It must be one of 'Infix', 
                  'Prefix' or 'Postfix'.
Terminator      : The row terminator.
ReleaseChar     : The release character.
Columns         : For an input link, specifies which columns to pass to the 
                  input card of the map.  Permitted values are 'All' and 'Selected'.  
                  Read documentation for more details.

*******************************************************************************/


public class WTXStage extends Processor
{
	private int              _inputLinkCount = 0;
	private int              _outputLinkCount = 0;
   private InputLink[]      _inputLink;
   private OutputLink[]     _outputLink;
   private OutputLink       _rejectLink = null;
   private WTXInputCard[]   _inputCard;
   private WTXOutputCard[]  _outputCard;
   private WTXMapRunner     _wtxRunner;
   private boolean          _isLittleEndian;
   private List<String>     _skippedColumns = null;
   private PrintWriter      _traceWriter = null;
   private FileOutputStream _traceStream = null; 
   private DirectByteArrayOutputStream _currentOutputStream;

   // Syntax objects for the current card
   private byte[]  _delimiterBytes;
   private byte[]  _terminatorBytes;
   private byte[]  _releaseCharBytes;
   private String  _delimiterSearchPattern;
   private String  _terminatorSearchPattern;
   private String  _releaseCharSearchPattern;
   private String  _delimiterReplacement;
   private String  _terminatorReplacement;
   private String  _releaseCharReplacement;
   private String  _charset;
   private int     _delimLocation;

   // Stage properties
   private String  _logDirectory;
   private String  _mapFile;
   private String  _resourceFile;
   private String  _traceFile;
   private boolean _failOnWarning;
   private boolean _fRunEachRow;
   private boolean _fDebug;
   private Boolean _fMapTrace = null;
   private Boolean _fMapAudit = null;

   // Formatters for times and timestamps
   private DateFormat _timeNoMicroseconds;
   private DateFormat _timestampNoMicroseconds;
   private DateFormat _timeWithMicroseconds;
   private DateFormat _timestampWithMicroseconds;

   // Delimiter location values
   private final static int DELIM_LOCATION_INFIX = 1;
   private final static int DELIM_LOCATION_PREFIX = 2;
   private final static int DELIM_LOCATION_POSTFIX = 3;

   // The property definitions.
   private String[][] _userPropertyDefinitions = 
   {
      // Stage properties
      {"MapFile", null, "Map file", "The full path for the TX map file.", "S"},
      {"LogDir", null, "Log directory", "The path of the directory where log files will be written.", "S"},
      {"ResourceFile", null, "Resource file", "The path of the resource file.", "S"},
      {"RunEach", "Wave", "Run each", "Specifies whether to run the map at the end of the wave, "+
             "or to run the map for each input row.  Valid values are 'Wave' and 'Row'.", "S"},
      {"MapTrace", null, "Map trace", "Turn map trace on or off.  Valid values are 'On' and 'Off'", "S"},
      {"MapAudit", null, "Map audit", "Turn map audit on or off.  Valid values are 'On' and 'Off'", "S"},
      {"FailOnWarning", "false", "Fail on warning", "Fail the job if the map returns a warning.", "S"},
      {"Debug", "false", "Debug", "If set to 'true' additional log messages are produced.", "S"},
      {"TraceFile", null, "Trace file", "Specifies the name of a trace file to which trace information "+
             "will be written.", "S"},

      // Link properties
      {"Card", null, "Card number", "The number of the card in the map (offset from 1).", "L"},
      {"Charset", null, "Charset", "The character set for string data.  If not set the platform default is used.", "L"},
      {"UseStrings", "false", "Use strings", "If 'true' all columns will be transferred as strings.", "L"},
      {"Delimiter", null, "Delimiter", "The column delimiter.", "L"},
      {"Location", "Infix", "Delimiter location", "The location of the column delimiter.  It must be one of 'Infix', "+
         "'Prefix' or 'Postfix'.", "L"},
      {"Terminator", null, "Terminator", "The row terminator.", "L"},
      {"ReleaseChar", null, "Release character", "The release character.", "L"},
      {"Columns", "All", "Include columns", 
         "For an input link, specifies which columns to pass to the input card of the map. "+
         "Permitted values are 'All' and 'Selected'.  Read documentation for more details.", "L"},
   };

   // Allow square brackets or angled brackets, since the stage editor has an issue with
   // angled brackets.
   private String[][] _escapeLiterals = 
   {
      {"<LF>",   "\n"},
      {"<CR>",   "\r"},
      {"<HT>",   "\t"},
      {"<NULL>", "\0"},
      {"<SP>",   " "},
      {"<WSP>",  " "},
      {"<NLW>",  "\r\n"},
      {"<NLU>",  "\n"},

      {"[LF]",   "\n"},
      {"[CR]",   "\r"},
      {"[HT]",   "\t"},
      {"[NULL]", "\0"},
      {"[SP]",   " "},
      {"[WSP]",  " "},
      {"[NLW]",  "\r\n"},
      {"[NLU]",  "\n"},
   };

   public WTXStage()
   {
      super();
      logEntry();
      logExit();
   }

   public Capabilities getCapabilities()
   {
      logEntry();
      Capabilities capabilities = new Capabilities();
      capabilities.setMaximumInputLinkCount(-1);
      capabilities.setMaximumOutputStreamLinkCount(-1);
      capabilities.setMaximumRejectLinkCount(1);
      logExit();
      return capabilities;
   }

   public List<PropertyDefinition> getUserPropertyDefinitions()
   {
      logEntry();
      ArrayList<PropertyDefinition> list = new ArrayList<PropertyDefinition>();
      for (int i = 0;  i < _userPropertyDefinitions.length;  i++)
      {
         list.add(new PropertyDefinition(_userPropertyDefinitions[i][0],
                   _userPropertyDefinitions[i][1],
                   _userPropertyDefinitions[i][2],
                   _userPropertyDefinitions[i][3],
                   (_userPropertyDefinitions[i][4].equals("S") ?
                          PropertyDefinition.Scope.STAGE : PropertyDefinition.Scope.LINK)));
      }
      logExit();
      return list;
   }


   // Validate the properties and ensure they match the stage configuration
   public boolean validateConfiguration(Configuration configuration, 
                                        boolean       isRuntime) throws Exception
   {
      logEntry();

      _inputLinkCount = configuration.getInputLinkCount();
      _outputLinkCount = configuration.getStreamOutputLinkCount();

      // Create arrays of links and card objects
      if (_inputLinkCount > 0)
      {
         _inputLink = configuration.getInputLinks().toArray(new InputLink[0]);
         _inputCard = new WTXInputCard[_inputLinkCount];
      }

      if (_outputLinkCount > 0)
      {
         _outputLink = new OutputLink[_outputLinkCount];
         for (int i = 0;  i < _outputLinkCount;  i++)
         {
            _outputLink[i] = configuration.getStreamOutputLink(i);
         }
         _outputCard = new WTXOutputCard[_outputLinkCount];
      }

      // Parse the properties and create the card objects
      processProperties(configuration);

      if (configuration.getRejectLinkCount() > 0)
      {
         if (!_fRunEachRow)
         {
            Logger.warning("Reject link is only applicable when the stage is configured to run "+
                           "a map for each input row.");
         }

         // Find the reject link
         for (OutputLink link : configuration.getOutputLinks())
         {
            if (link.getLinkType() == Link.LinkType.REJECT)
            {
               _rejectLink = link;
               break;
            }
         }
      }

      logExit();
      return true;
   }


   // Set up TX interface and load the map
   public void initialize()
   {
      try
      {
         // initialize WTX      
         _wtxRunner = new WTXMapRunner(_logDirectory, _resourceFile, _fMapTrace, _fMapAudit);

         // load a WTX map
         _wtxRunner.loadMap(_mapFile);

         // Associate the card objects with the map
         for (int link = 0;  link < _inputLinkCount;  link++)
         {
            _wtxRunner.overrideInput(_inputCard[link]);
         }
         for (int link = 0;  link < _outputLinkCount;  link++)
         {
            _wtxRunner.overrideOutput(_outputCard[link]);
         }
      }
      catch (Exception e)
      {
         if (_fDebug)
         {
            logException(e);
         }
         Logger.fatal(e.getMessage());
      }
   }


   public void terminate() throws Exception
   {
      // Clean up
      _wtxRunner.unload();
      _wtxRunner.close();
   }


   // Main processing method
   public void process()
   {
      // Determine the endianness
      _isLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

      // Set up the time and timestamp formatters
      _timeNoMicroseconds = new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH);
      _timestampNoMicroseconds = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
      _timeWithMicroseconds = new SimpleDateFormat("HH:mm:ss.S", Locale.ENGLISH);
      _timestampWithMicroseconds = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S", Locale.ENGLISH);

      // Create a trace file if the option was specified
      if (_traceFile != null)
      {
         try
         {
            _traceStream = new FileOutputStream(_traceFile, false);
            _traceWriter = new PrintWriter(_traceStream);
         }
         catch (Exception e)
         {
            Logger.warning("Could not open trace file "+_traceFile);
            _traceStream = null;
            _traceWriter = null;
         }
      }

      String fatalMessage = null;
      try
      {
         // Read in all the data from the input links
         for (int link = 0;  link < _inputLinkCount;  link++)
         {
            processInputLink(link);
         }

         if (!_fRunEachRow)
         {
            runTheMap();
         }
      }
      catch (Exception e)
      {
         if (_fDebug)
         {
            logException(e);
         }
         fatalMessage = e.getMessage();
      }

      if (_traceWriter != null)
      {
         try
         {
            _traceWriter.close();
            _traceStream.close();
         }
         catch (Exception e)
         {
            ;
         }
      }

      if (fatalMessage != null)
      {
         Logger.fatal(fatalMessage);
      }
   }


   // Runs a map
   private WTXMapExecutionResults runTheMap() throws Exception
   {
      // Provide output streams for TX to populate on output
      for (int link = 0;  link < _outputLinkCount;  link++)
      {
         DirectByteArrayOutputStream outputByteStream = new DirectByteArrayOutputStream();
         _outputCard[link].setOutputStream(outputByteStream);
      }

      // Run the map
      WTXMapExecutionResults results = _wtxRunner.executeMap();

      if (_fDebug)
      {
         Logger.information("Executed map.  Status (" + results.getResultCode() + "): " + results.getResponseMessage());
      }

      if (results.isWarning())
      {
         String warningMsg = "Map returned warning (" + results.getResultCode() + 
                                           "): " + results.getResponseMessage();
         if (_failOnWarning)
         {
            throw new Exception(warningMsg);
         }
         else
         {
            Logger.warning(warningMsg);
         }
      }

      // Get the results
      if (results.isSuccess() || results.isWarning())
      {
         for (int link = 0;  link < _outputLinkCount;  link++)
         {
            processOutputLink(link);
         }
      }
      else
      {
         // If there is a reject link (and we're running a map per row), then
         // we dont want to fail the job if a map run fails.
         if (_rejectLink == null)
         {
            throw new Exception("Map failed (" + results.getResultCode() + "): " + results.getResponseMessage());
         }
      }

      return results;
   }


   // Builds the data to send to the input card of the map
   private void processInputLink(int linkNum) throws Exception
   {
      // Allocate an output stream to write to
      DirectByteArrayOutputStream outputByteStream = new DirectByteArrayOutputStream();
      DataOutputStream outputStream = new DataOutputStream(outputByteStream);

      // Create a bytebuffer to perform byte ordering manipulations
      byte[] convertBuffer = new byte[8];
      ByteBuffer convertByteBuffer = ByteBuffer.wrap(convertBuffer);
      convertByteBuffer.order(ByteOrder.nativeOrder());

      InputLink inputLink = _inputLink[linkNum];

      // Get the charset for the card
      _charset = _inputCard[linkNum].getCharset();

      // Get the delimiters and release characters for the card
      setupSyntaxObjects(_inputCard[linkNum]);

      // Determine if values are transferred in binary form or string.
      boolean fUseStrings = _inputCard[linkNum].useStrings();

      // Create arrays for column class and whether time/timestamp have microseconds
      int numColumns = inputLink.getColumnMetadata().size();
      int[] sqlType = new int[numColumns];
      boolean[] hasMicroSeconds = new boolean[numColumns];
      Boolean[] stringPresentation = new Boolean[numColumns];
      int c = 0;
      for (ColumnMetadata column : inputLink.getColumnMetadata())
      {
         sqlType[c] = column.getSQLType();
         hasMicroSeconds[c] = column.hasMicrosecondResolution();
         stringPresentation[c] = isStringPresentation(column);
         c++;
      }

      if (linkNum == 0)
      {
         _skippedColumns = new ArrayList<String>();
      }

      // Determine which columns to pass to the map
      boolean[] skipColumn = new boolean[numColumns];
      c = 0;
      for (ColumnMetadata column : inputLink.getColumnMetadata())
      {
         if (_inputCard[linkNum].transferAll())
         {
            // If transfer all (Columns=All), then include unless [wtx-exclude] specified
            skipColumn[c] = (column.getDescription().indexOf("[wtx-exclude]") != -1);
         }
         else
         {
            // If not transfer all (Columns=Selected), then exclude unless [wtx-include] specified
            skipColumn[c] = (column.getDescription().indexOf("[wtx-include]") == -1);
         }

         if (_fDebug && skipColumn[c])
         {
            Logger.information("Excluding column "+column.getName()+" on input link "+linkNum);
         }

         // Store a list of column names that are being excluded.  The output link
         // processing uses this list to skip similarly named columns.  Only do this
         // for the first input link since column transfer is only supported for that.
         if (linkNum == 0 && skipColumn[c])
         {
            _skippedColumns.add(column.getName());
         }

         c++;
      }

      if (_traceWriter != null)
      {
         _traceWriter.println();
         _traceWriter.println("Processing input link "+linkNum+" (card "+_inputCard[linkNum].getCardNumber()+")");
      }

      // Loop around processing all incoming rows
      int row = 0;
      do
      {
         InputRecord inputRecord = inputLink.readRecord();
         if (inputRecord == null)
         {
            // No more input
            break;
         }

         if (_traceWriter != null)
         {
            _traceWriter.println();
            _traceWriter.println("Processing row "+row+".");
         }

         // If delimiter location is prefix, then put out a delimiter before any values
         if (_delimLocation == DELIM_LOCATION_PREFIX)
         {
            if (_delimiterBytes != null)
            {
               outputStream.write(_delimiterBytes);
            }
         }

         // Process this record
         for (int col = 0;  col < numColumns;  col++)
         {
            if (skipColumn[col])
            {
               continue;
            }

            if (_traceWriter != null)
            {
               _traceWriter.println("Column "+col+" "+inputLink.getColumn(col).getName()+":");
            }

            Object value = inputRecord.getValue(col);

            // Determine presentation from global value and column overrides
            boolean fOutputAsString = fUseStrings;
            if (stringPresentation[col] != null)
            {
               fOutputAsString = stringPresentation[col];
            }

            if (_traceWriter != null)
            {
               traceValue(value);
            }

            if (value != null)
            {
               switch(sqlType[col])
               {
               case ColumnMetadata.SQL_TYPE_CHAR:     // String
               case ColumnMetadata.SQL_TYPE_LONGVARCHAR:
               case ColumnMetadata.SQL_TYPE_VARCHAR:
               case ColumnMetadata.SQL_TYPE_WCHAR:
               case ColumnMetadata.SQL_TYPE_WLONGVARCHAR:
               case ColumnMetadata.SQL_TYPE_WVARCHAR:
               case ColumnMetadata.SQL_TYPE_UNKNOWN:
                  // Release delimiters and release chars
                  byte[] strBytes = getBytesFromString(escapeSyntax((String)value));
                  outputStream.write(strBytes, 0, strBytes.length);
                  break;

               case ColumnMetadata.SQL_TYPE_INTEGER:  // Long
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(((Long)value).toString()));
                  }
                  else if (_isLittleEndian)
                  {
                     outputStream.writeLong(Long.reverseBytes(((Long)value).longValue()));
                  }
                  else
                  {
                     outputStream.writeLong(((Long)value).longValue());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_BIT:      // Integer
               case ColumnMetadata.SQL_TYPE_SMALLINT: 
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(((Integer)value).toString()));
                  }
                  else if (_isLittleEndian)
                  {
                     outputStream.writeInt(Integer.reverseBytes(((Integer)value).intValue()));
                  }
                  else
                  {
                     outputStream.writeInt(((Integer)value).intValue());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_TINYINT:   // Short
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(((Short)value).toString()));
                  }
                  else if (_isLittleEndian)
                  {
                     outputStream.writeShort(Short.reverseBytes(((Short)value).shortValue()));
                  }
                  else
                  {
                     outputStream.writeShort(((Short)value).shortValue());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_DOUBLE:   // Double
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(((Double)value).toString()));
                  }
                  else if (_isLittleEndian)
                  {
                     convertByteBuffer.position(0);
                     convertByteBuffer.putDouble(((Double)value).doubleValue());
                     outputStream.write(convertBuffer, 0, 8);
                  }
                  else
                  {
                     outputStream.writeDouble(((Double)value).doubleValue());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_FLOAT:    // Float
               case ColumnMetadata.SQL_TYPE_REAL:
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(((Float)value).toString()));
                  }
                  else if (_isLittleEndian)
                  {
                     convertByteBuffer.position(0);
                     convertByteBuffer.putFloat(((Float)value).floatValue());
                     outputStream.write(convertBuffer, 0, 4);
                  }
                  else
                  {
                     outputStream.writeFloat(((Float)value).floatValue());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_BINARY:   // byte[]
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(convertToHex((byte[])value)));;
                  }
                  else
                  {
                     outputStream.write((byte[])value, 0, ((byte[])value).length);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_VARBINARY: // byte[]
               case ColumnMetadata.SQL_TYPE_LONGVARBINARY:
                  if (fOutputAsString)
                  {
                     outputStream.write(getBytesFromString(convertToHex((byte[])value)));
                  }
                  else
                  {
                     // Output the size as a 4 byte integer
                     if (_isLittleEndian)
                     {
                        outputStream.writeInt(Integer.reverseBytes(((byte[])value).length));
                     }
                     else
                     {
                        outputStream.writeInt(((byte[])value).length);
                     }
                     if (_delimiterBytes != null)
                     {
                        outputStream.write(_delimiterBytes, 0, _delimiterBytes.length);
                     }
                     outputStream.write((byte[])value, 0, ((byte[])value).length);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_BIGINT:   // java.math.BigInteger 
                  outputStream.write(getBytesFromString(((BigInteger)value).toString()));
                  break;

               case ColumnMetadata.SQL_TYPE_DECIMAL:  // java.math.BigDecimal
               case ColumnMetadata.SQL_TYPE_NUMERIC:
                  outputStream.write(getBytesFromString(((BigDecimal)value).toString()));
                  break;

               case ColumnMetadata.SQL_TYPE_DATE:     // java.sql.Date
                  outputStream.write(getBytesFromString(((Date)value).toString()));   
                  break;

               case ColumnMetadata.SQL_TYPE_TIME:     // java.sql.Time
                  if (hasMicroSeconds[col])
                  {
                     outputStream.write(getBytesFromString(_timeWithMicroseconds.format((Time)value)));
                  }
                  else
                  {
                     outputStream.write(getBytesFromString(_timeNoMicroseconds.format((Time)value)));
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_TIMESTAMP: // java.sql.Timestamp
                  if (hasMicroSeconds[col])
                  {
                     outputStream.write(getBytesFromString(_timestampWithMicroseconds.format((Timestamp)value)));
                  }
                  else
                  {
                     outputStream.write(getBytesFromString(_timestampNoMicroseconds.format((Timestamp)value)));
                  }
                  break;

               default:
                  throw new Exception("Unrecognized metadata type");
               }
            }
            else
            {
               // For nulls, the only case to handle is to write out the
               // size as 0 for varbinary items.
               if (sqlType[col] == ColumnMetadata.SQL_TYPE_VARBINARY ||
                   sqlType[col] == ColumnMetadata.SQL_TYPE_LONGVARBINARY)
               {
                  // Output the size as a 4 byte integer
                  if (_isLittleEndian)
                  {
                     outputStream.writeInt(Integer.reverseBytes(0));
                  }
                  else
                  {
                     outputStream.writeInt(0);
                  }

                  if (_delimiterBytes != null)
                  {
                     outputStream.write(_delimiterBytes);
                  }
               }
            }

            // Don't output a column delimiter for the last column unless postfix
            if (_delimiterBytes != null && 
                (_delimLocation == DELIM_LOCATION_POSTFIX || col < numColumns-1))
            {
               outputStream.write(_delimiterBytes);
            }
         }

         if (_terminatorBytes != null)
         {
            outputStream.write(_terminatorBytes);
         }

         if (_fRunEachRow)
         {
            // Provide the data to the map
            outputStream.close();
            WTXInputCard card = _inputCard[linkNum];
            card.setInputData(outputByteStream.getByteArray(), outputByteStream.getCount());
            if (_fDebug)
            {
               Logger.information("Input link "+linkNum+" (card "+card.getCardNumber()+") "+
                                   "produced "+outputByteStream.getCount()+" bytes.");
            }
            if (_traceWriter != null)
            {
               _traceWriter.println("Input link "+linkNum+" (card "+card.getCardNumber()+") "+
                                    "produced "+outputByteStream.getCount()+" bytes.");
            }

            WTXMapExecutionResults results = runTheMap();
            if (_rejectLink != null && 
                (results.isError() || (results.isWarning() && _failOnWarning)))
            {
               // Reject the row
               RejectRecord rejectRecord = _rejectLink.getRejectRecord(inputRecord);
               rejectRecord.setErrorText(results.getResponseMessage());
               rejectRecord.setErrorCode(results.getResultCode());
               _rejectLink.writeRecord(rejectRecord);               
            }

            // Discard current content to start fresh next row
            outputByteStream.reset();
         }

         row++;

      } while (true);

      if (!_fRunEachRow)
      {
         // Provide the data to the map
         outputStream.flush();
         WTXInputCard card = _inputCard[linkNum];
         card.setInputData(outputByteStream.getByteArray(), outputByteStream.getCount());
         if (_fDebug)
         {
            Logger.information("Input link "+linkNum+" (card "+card.getCardNumber()+") "+
                                "produced "+outputByteStream.getCount()+" bytes.");
         }
         if (_traceWriter != null)
         {
            _traceWriter.println("Input link "+linkNum+" (card "+card.getCardNumber()+") "+
                                 "produced "+outputByteStream.getCount()+" bytes.");
         }
      }

      try
      {
         // 2nd parm is append flag.
         FileOutputStream fs = new FileOutputStream("c:\\temp\\data.txt", true);
         fs.write(outputByteStream.getByteArray(), 0, outputByteStream.getCount());
         fs.close();        
      }
      catch (Exception e)
      {
      }
   }


   // Parses the data produced from an output card of the map
   private void processOutputLink(int linkNum) throws Exception
   {
      // Get the output stream for the output link
      DirectByteArrayOutputStream outputStream = 
                (DirectByteArrayOutputStream) _outputCard[linkNum].getOutputStream();

      if (_fDebug)
      {
         Logger.information("Output link "+linkNum+" (card "+_outputCard[linkNum].getCardNumber()+") "+
                            "contains "+outputStream.getCount()+" bytes.");
      }

      if (_traceWriter != null)
      {
         _traceWriter.println();
         _traceWriter.println("Processing output link "+linkNum+" (card "+_outputCard[linkNum].getCardNumber()+")");
         _traceWriter.println("Contains "+outputStream.getCount()+" bytes.");
      }

      // If the output card produced no bytes, then do no further processing
      if (outputStream.getCount() == 0)
      {
         return;
      }

      // Create a ByteBuffer to parse the output data and set the byte order to native
      ByteBuffer buffer = ByteBuffer.wrap(outputStream.getByteArray(), 0, outputStream.getCount());
      buffer.order(ByteOrder.nativeOrder());

      // Store the output stream object for use by parseString
      _currentOutputStream = outputStream;

      // Get the charset for the card
      _charset = _outputCard[linkNum].getCharset();

      // Get the delimiters and release characters for the card
      setupSyntaxObjects(_outputCard[linkNum]);

      // Determine if values are transferred in binary form or string.
      boolean fUseStrings = _outputCard[linkNum].useStrings();

      // Create an array that holds the column class for each column and the max length
      OutputLink outputLink = _outputLink[linkNum];
      int numColumns = outputLink.getColumnMetadata().size();
      int[] sqlType = new int[numColumns];
      int[] length = new int[numColumns];
      Boolean[] stringPresentation = new Boolean[numColumns];

      int c = 0;
      for (ColumnMetadata column : outputLink.getColumnMetadata())
      {
         sqlType[c] = column.getSQLType();

         // Set the field length.
         if (sqlType[c] == ColumnMetadata.SQL_TYPE_DATE)
         {
            length[c] = 10;
         }
         else if (sqlType[c] == ColumnMetadata.SQL_TYPE_TIME)
         {
            // "HH:MM:SS" or "HH:MM:SS.FFFFFF"
            length[c] = column.hasMicrosecondResolution() ? 15 : 8;
         }
         else if (sqlType[c] == ColumnMetadata.SQL_TYPE_TIMESTAMP)
         {
            // "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.FFFFFF"
            length[c] = column.hasMicrosecondResolution() ? 26 : 19;
         }
         else
         {
            length[c] = column.getPrecision();
         }

         // Get a presentation attribute if specified in the description
         stringPresentation[c] = isStringPresentation(column);
         c++;
      }

      // If columns are being transferred from the input link to the output mark them as skipped.
      boolean[] skipColumn = new boolean[numColumns];
      c = 0;
      for (ColumnMetadata column : outputLink.getColumnMetadata())
      {
         skipColumn[c] = (_skippedColumns != null && _skippedColumns.contains(column.getName()));
         if (_fDebug && skipColumn[c])
         {
            Logger.information("Excluding column "+column.getName()+" on output link "+linkNum);
         }
         c++;
      }

      // Loop around until we've parsed all the data
      int row = 0;
      do
      {
         int col = 0;
         try
         {
            if (_traceWriter != null)
            {
               _traceWriter.println();
               _traceWriter.println("Processing row "+row+".");
            }

            OutputRecord outputRecord = outputLink.getOutputRecord();

            // Consume a delimiter if location is prefix
            if (_delimLocation == DELIM_LOCATION_PREFIX && _delimiterBytes != null)
            {
               buffer.position(buffer.position()+_delimiterBytes.length);
            }

            for (col = 0;  col < numColumns;  col++)
            {
               if (skipColumn[col])
               {
                  continue;
               }

               if (_traceWriter != null)
               {
                  _traceWriter.println("Column "+col+" "+outputLink.getColumn(col).getName()+":");
               }

               Object value = null;


               // Determine presentation from global value and column overrides
               boolean fInputAsString = fUseStrings;
               if (stringPresentation[col] != null)
               {
                  fInputAsString = stringPresentation[col];
               }


               // Calculate the next delimiter
               boolean fLastColumn = (col == numColumns - 1);

               byte[] delimiter = (!fLastColumn || _delimLocation == DELIM_LOCATION_POSTFIX)
                                  ? _delimiterBytes : _terminatorBytes;

               switch(sqlType[col])
               {
               case ColumnMetadata.SQL_TYPE_CHAR:     // String
               case ColumnMetadata.SQL_TYPE_LONGVARCHAR:
               case ColumnMetadata.SQL_TYPE_VARCHAR:
               case ColumnMetadata.SQL_TYPE_WCHAR:
               case ColumnMetadata.SQL_TYPE_WLONGVARCHAR:
               case ColumnMetadata.SQL_TYPE_WVARCHAR:
               case ColumnMetadata.SQL_TYPE_UNKNOWN:
                  value = parseString(buffer, delimiter, length[col], fLastColumn);
                  break;

               case ColumnMetadata.SQL_TYPE_INTEGER:  // Long
                  if (fInputAsString)
                  {
                     value = Long.valueOf(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     value = Long.valueOf(buffer.getLong());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_BIT:      // Integer
               case ColumnMetadata.SQL_TYPE_SMALLINT: 
                  if (fInputAsString)
                  {
                     value = Integer.valueOf(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     value = Integer.valueOf(buffer.getInt());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_TINYINT:   // Short
                  if (fInputAsString)
                  {
                     value = Short.valueOf(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     value = Short.valueOf(buffer.getShort());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_DOUBLE:   // Double
                  if (fInputAsString)
                  {
                     value = Double.valueOf(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     value = Double.valueOf(buffer.getDouble());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_FLOAT:    // Float
               case ColumnMetadata.SQL_TYPE_REAL:
                  if (fInputAsString)
                  {
                     value = Float.valueOf(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     value = Float.valueOf(buffer.getFloat());
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_BINARY:   // byte[]
                  if (fInputAsString)
                  {
                     value = convertFromHex(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     value = getBytes(buffer, length[col]);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_VARBINARY:
               case ColumnMetadata.SQL_TYPE_LONGVARBINARY:
                  if (fInputAsString)
                  {
                     value = convertFromHex(parseString(buffer, delimiter, length[col], fLastColumn));
                  }
                  else
                  {
                     // Get the size and the column delimiter
                     int len = buffer.getInt();
                     buffer.position(buffer.position()+_delimiterBytes.length);
                     value = getBytes(buffer, len);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_BIGINT:   // java.math.BigInteger 
                  value = parseString(buffer, delimiter, length[col], fLastColumn);
                  if (value != null)
                  {
                     value = new BigInteger((String)value);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_DECIMAL:  // java.math.BigDecimal
               case ColumnMetadata.SQL_TYPE_NUMERIC:
                  value = parseString(buffer, delimiter, length[col], fLastColumn);
                  if (value != null)
                  {
                     value = new BigDecimal((String)value);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_DATE:     // java.sql.Date
                  value = parseString(buffer, delimiter, length[col], fLastColumn);
                  if (value != null)
                  {
                     value = Date.valueOf((String)value);
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_TIME:     // java.sql.Time
                  String timeStr = parseString(buffer, delimiter, length[col], fLastColumn);
                  if (timeStr != null)
                  {
                     if (timeStr.length() == 8)          // "HH:MM:SS"
                     {
                        value = Time.valueOf(timeStr);
                     }
                     else
                     {
                        value = _timeWithMicroseconds.parse(timeStr);
                     }
                  }
                  break;

               case ColumnMetadata.SQL_TYPE_TIMESTAMP: // java.sql.Timestamp
                  String timestampStr = parseString(buffer, delimiter, length[col], fLastColumn);
                  if (timestampStr != null)
                  {
                     if (timestampStr.length() == 19)      // "YYYY-MM-DD HH:MM:SS"
                     {
                        value = Timestamp.valueOf(timestampStr);
                     }
                     else
                     {
                        value = new Timestamp(_timestampWithMicroseconds.parse(timestampStr).getTime());
                     }
                  }
                  break;

               default:
                  throw new Exception("Unrecognized metadata type");
               }

               // Skip the column delimiter
               if (_delimiterBytes != null && 
                   (_delimLocation == DELIM_LOCATION_POSTFIX || col < numColumns-1))
               {
                  buffer.position(buffer.position()+_delimiterBytes.length);
               }

               if (_traceWriter != null)
               {
                  traceValue(value);
               }

               // Set the column value
               outputRecord.setValue(col, value);
            }

            // Skip the row delimiter
            if (_terminatorBytes != null && buffer.remaining() > 0)
            {
                  buffer.position(buffer.position()+_terminatorBytes.length);
            }

            // Output the record
            outputLink.writeRecord(outputRecord);
            if (buffer.remaining() == 0)
            {
               // All data has been read
               break;
            }
         }
         catch (ExhaustedDataException edEx)
         {
            if (_fDebug)
            {
               logException(edEx);
            }
            if (_traceWriter != null)
            {
               _traceWriter.println("Insufficient data at offset "+edEx.getPosition()+
                                    " for column "+outputLink.getColumn(col).getName());
            }
            Logger.fatal("Insufficient data at offset "+edEx.getPosition()+
                         " for column "+outputLink.getColumn(col).getName());
            break;
         }
         catch (BufferUnderflowException bufEx)
         {
            if (_fDebug)
            {
               logException(bufEx);
            }
            if (_traceWriter != null)
            {
               _traceWriter.println("Insufficient data for column "+outputLink.getColumn(col).getName());
            }
            Logger.fatal("Insufficient data for column "+outputLink.getColumn(col).getName());
            break;
         }

         row++;

      } while (true);
   }


   // Looks for PRESENTATION(char) or PRESENTATION(binary) in the description.
   // Returns null if neither is found.
   private Boolean isStringPresentation(ColumnMetadata column) throws Exception
   {
      Boolean presentation = null;
      String desc = column.getDescription();
      int idx = desc.indexOf("PRESENTATION(");
      if (idx != -1)
      {
         // 13 is length of "PRESENTATION("
         int idx2 = desc.indexOf(')', 13);
         if (idx2 != -1)
         {
            String pres = desc.substring(idx+13, idx2);
            if (pres.equalsIgnoreCase("CHAR"))
            {
               presentation = new Boolean(true);
            }
            else if (pres.equalsIgnoreCase("BINARY"))
            {
               presentation = new Boolean(false);
            }
            else
            {
               throw new Exception("Presentation specifier '" + pres + "' for column "+
                                   column.getName()+" is invalid.  It must be 'char' or 'binary'");
            }
         }
         else
         {
            throw new Exception("Presentation specifier for column "+column.getName()+" is malformed");
         }
      }
      return presentation;
   }


   private void traceValue(Object value)
   {
      String debugValue;
      if (value == null)
      {
         debugValue = "NULL";
      }
      else if (value instanceof byte[])
      {
         debugValue = "{"+convertToHex((byte[])value)+"}";
      }
      else
      {
         debugValue = value.toString();
      }
      _traceWriter.println("   Value="+debugValue);
   }


   // Sets the member variables that hold the syntax objects used to process the cards
   private void setupSyntaxObjects(WTXCard card) throws Exception
   {
      String delimiter = card.getDelimiter();
      String terminator = card.getTerminator();
      String releaseChar = card.getReleaseChar();

      // Get the bytes to write out
      _delimiterBytes = getBytesFromString(delimiter);
      _terminatorBytes = getBytesFromString(terminator);
      _releaseCharBytes = getBytesFromString(releaseChar);

      // Get the search patterns
      _delimiterSearchPattern = getSearchPattern(delimiter);
      _terminatorSearchPattern = getSearchPattern(terminator);
      _releaseCharSearchPattern = getSearchPattern(releaseChar);

      // Get the replacement strings
      _delimiterReplacement = getReplacementString(releaseChar+delimiter);
      _terminatorReplacement = getReplacementString(releaseChar+terminator);
      _releaseCharReplacement = getReplacementString(releaseChar+releaseChar);

      // Get other attributes about delimiters
      _delimLocation = card.getDelimiterLocation();
   }


   private String getSearchPattern(String literal)
   {
      if (literal == null)
      {
         return null;
      }

      return Pattern.quote(literal);
   }


   private String getReplacementString(String literal)
   {
      if (literal == null)
      {
         return null;
      }

      return Matcher.quoteReplacement(literal);
   }


   // Converts hex string to bytes
   public static byte[] convertFromHex(String value)
   {
      if (value == null)
      {
         return null;
      }

      int len = value.length();
      byte[] data = new byte[len / 2];
      
      for (int i = 0;  i < len;  i += 2) 
      {
         data[i/2] = (byte) ((Character.digit(value.charAt(i), 16) << 4) +
                              Character.digit(value.charAt(i+1), 16));
      }
      return data;
   }


   // Coverts bytes to hex string
   public static String convertToHex(byte[] value) 
   {
      if (value == null)
      {
         return null;
      }

      final char[] hexCode = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
      char[] hexChars = new char[value.length * 2];
      
      int outIndex = 0;
      for (int i = 0;  i < value.length;  i++) 
      {
         int v = value[i] & 0xFF;
         hexChars[outIndex++] = hexCode[v >>> 4];
         hexChars[outIndex++] = hexCode[v & 0x0F];
      }
      return new String(hexChars);
   }


   // Gets bytes from the ByteBuffer
   private static byte[] getBytes(ByteBuffer buffer, int len) throws Exception
   {
      byte[] bytes = new byte[len];
      for (int i = 0;  i < len;  i++)
      {
         bytes[i] = buffer.get();
      }
      return bytes;
   }


   // Parse a string value from the output card data.
   private String parseString(ByteBuffer buffer, byte[] delimiter, int length, boolean fLastColumn) 
                                throws ExhaustedDataException, UnsupportedEncodingException
   {
      String ret = null;

      // If there is no delimiter, then use length to get fixed-width string
      if (delimiter == null)
      {
         byte[] bytes = _currentOutputStream.getByteArray();
         int currentPos = buffer.position();

         if (_traceWriter != null)
         {
            _traceWriter.println("   Offset "+currentPos+": Reading "+length+" characters.");
         }

         if (_charset == null)
         {
            ret = new String(bytes, buffer.position(), buffer.remaining());
         }
         else
         {
            ret = new String(bytes, buffer.position(), buffer.remaining(), _charset);
         }
         ret = ret.substring(0, length);

         // Adjust the position of the ByteBuffer - see how many bytes were consumed by the string
         int consumed;
         if (_charset == null)
         {
            consumed = ret.getBytes().length;
         }
         else
         {
            consumed = ret.getBytes(_charset).length;
         }
         buffer.position(currentPos+consumed);
      }
      else
      {
         int currentPos = buffer.position();
         if (_traceWriter != null)
         {
            String delimString = (_charset == null) ? new String(delimiter) : new String(delimiter, _charset);
            _traceWriter.println("   Offset "+currentPos+": Looking for delimiter '"+delimString+"'.");
         }

         int delimPos = findDelimiter(buffer, delimiter);
         if (delimPos == -1)
         {
            if (_traceWriter != null)
            {
               _traceWriter.println("   Delimiter not found.");
            }

            // If this is the last column of the row tolerate there not being a row delimiter.
            if (!fLastColumn)
            {
               throw new ExhaustedDataException(currentPos);
            }
            else
            {
               delimPos = currentPos + buffer.remaining();
            }
         }
         else if (_traceWriter != null)
         {
            _traceWriter.println("   Offset "+delimPos+": Delimiter found.");
         }

         int len = delimPos - currentPos;
         int finalLen = len;
         if (len > 0)
         {
            byte[] bytes = new byte[len];
            for (int i = 0;  i < len;  i++)
            {
               bytes[i] = buffer.get();
            }

            // Scan the byte array and remove any release characters
            if (_releaseCharBytes != null)
            {
               for (int i = 0;  i < len;  i++)
               {
                  if (bytes[i] == _releaseCharBytes[0])
                  {
                     // The first byte matches - check the rest
                     boolean fFound = true;
                     for (int j = 1;  j < _releaseCharBytes.length;  j++)
                     {
                        if (bytes[i+j] != _releaseCharBytes[j])
                        {
                           fFound = false;
                           break;
                        }
                     }

                     if (fFound)
                     {
                        // move up all the bytes
                        finalLen -= _releaseCharBytes.length;
                        for (int j = i;  j < finalLen;  j++)
                        {
                           bytes[j] = bytes[j+_releaseCharBytes.length];
                        }
                     }
                  }
               }
            }

            if (_charset != null)
            {
               ret = new String(bytes, 0, finalLen, _charset);
            }
            else
            {
               ret = new String(bytes, 0, finalLen);
            }
         }
      }

      return ret;
   }


   // Find the specified delimiter in the data.
   private int findDelimiter(ByteBuffer buffer, byte[] delim)
   {
      int pos = -1;
      boolean fFound = false;
      int savedPosition = buffer.position();

      while (true)
      {
         try
         {
            byte x = buffer.get();
            if (x == delim[0])
            {
               pos = buffer.position()-1;
               fFound = true;

               // First byte matched, compare the rest
               for (int j = 1;  j < delim.length;  j++)
               {
                  if (buffer.get() != delim[j])
                  {
                     fFound = false;
                     buffer.position(pos+1);
                     break;
                  }
               }

               if (fFound && _releaseCharBytes != null)
               {
                  // Check for release characters.  First make sure there are
                  // enough potential chars before the delimiter to hold the 
                  // release
                  if ((pos - savedPosition) >= _releaseCharBytes.length)
                  {
                     buffer.position(pos - _releaseCharBytes.length);
                     boolean fFoundRelease = true;
                     for (int r = 0;  r < _releaseCharBytes.length;  r++)
                     {
                        if (buffer.get() != _releaseCharBytes[r])
                        {
                           fFoundRelease = false;
                           break;
                        }
                     }

                     if (fFoundRelease)
                     {
                        // We found a release, so we didn't really find the 
                        // delimiter.
                        fFound = false;
                        buffer.position(pos+1);
                     }
                  }
               }

               if (fFound)
               {
                  // We're done - we found the delimiter.
                  break;
               }
            }
         }
         catch (BufferUnderflowException e)
         {
            // Did not find the delimiter
            fFound = false;
            break;
         }
      }

      buffer.position(savedPosition);

      if (!fFound)
      {
         pos = -1;
      }

      return pos;
   }


   // Get bytes from the String value using the specified charset
   private byte[] getBytesFromString(String value) throws UnsupportedEncodingException
   {
      byte[] bytesValue = null;
      if (value != null)
      {
         if (_charset == null)
         {
            bytesValue = value.getBytes();
         }
         else
         {
            bytesValue = value.getBytes(_charset);
         }
      }
      return bytesValue;
   }


   // Replace escaped syntax in the output data
   private String escapeSyntax(String source)
   {
      // If no release char is defined then do nothing
      if (_releaseCharBytes == null)
      {
         return source;
      }

      // Substitute the release char itself
      String result = source.replaceAll(_releaseCharSearchPattern, _releaseCharReplacement);

      // Substitute the row delimiter
      if (_terminatorBytes != null)
      {
         result = result.replaceAll(_terminatorSearchPattern, _terminatorReplacement);
      }

      // Substitute the column delimiter
      if (_delimiterBytes != null)
      {
         result = result.replaceAll(_delimiterSearchPattern, _delimiterReplacement);
      }

      return result;
   }


   // Gather the property values from the configuration
   private void processProperties(Configuration config) throws Exception
   {
      // Get the stage properties
      Properties properties = config.getUserProperties();
      _mapFile = getProperty(properties, "MapFile", true);
      _logDirectory = getProperty(properties, "LogDir", false);

      _resourceFile = getProperty(properties, "ResourceFile", false);
      _failOnWarning = getBooleanProperty(properties, "FailOnWarning", false, false);
      _fDebug = getBooleanProperty(properties, "Debug", false, false);
      _traceFile = getProperty(properties, "TraceFile", false);

      if (getProperty(properties, "MapTrace", false) != null)
      {
         _fMapTrace = getBooleanProperty(properties, "MapTrace", false, false);
      }

      if (getProperty(properties, "MapAudit", false) != null)
      {
         _fMapAudit = getBooleanProperty(properties, "MapAudit", false, false);
      }

      _fRunEachRow = false;
      String runEach = getProperty(properties, "RunEach", false);
      if (runEach != null)
      {
         if (runEach.equalsIgnoreCase("Row"))
         {
            _fRunEachRow = true;
            if (config.getInputLinks().size() > 1)
            {
               throw new Exception("RunEach cannot be set to 'Row' if there are multiple input links.");
            }
         }
         else if (!runEach.equalsIgnoreCase("Wave"))
         {
            throw new Exception("Property 'RunEach' must have value 'Row' or 'Wave'");
         }
      }

      // Process all the input links
      for (int i = 0;  i < _inputLinkCount;  i++)
      {
         Link link = config.getInputLink(i);
         properties = link.getUserProperties();

         Integer cardNum = getIntegerProperty(properties, "Card", true);
         WTXInputCard card = new WTXInputCard(i, cardNum);
         _inputCard[i] = card;
         setCardProperties(properties, card);
      }

      // Process all the output links
      for (int i = 0;  i < _outputLinkCount;  i++)
      {
         Link link = config.getOutputLink(i);
         properties = link.getUserProperties();

         Integer cardNum = getIntegerProperty(properties, "Card", true);
         WTXOutputCard card = new WTXOutputCard(i, cardNum);
         _outputCard[i] = card;
         setCardProperties(properties, card);
      }
   }


   // Store the properties for the card
   private void setCardProperties(Properties properties, WTXCard card) throws Exception
   {
      String val;
      val = getProperty(properties, "Delimiter", false);
      if (val != null)
      {
         val = substituteEscapes(val);
         card.setDelimiter(val);
      }

      val = getProperty(properties, "Terminator", false);
      if (val != null)
      {
         val = substituteEscapes(val);
         card.setTerminator(val);
      }

      val = getProperty(properties, "ReleaseChar", false);
      if (val != null)
      {
         val = substituteEscapes(val);
         card.setReleaseChar(val);
      }

      val = getProperty(properties, "Location", false);
      if (val != null)
      {
         // Ensure the location property is one of prefix, postfix or infix
         int location = DELIM_LOCATION_INFIX;
         if (val.equalsIgnoreCase("infix"))
         {
            location = DELIM_LOCATION_INFIX;
         }
         else if (val.equalsIgnoreCase("prefix"))
         {
            location = DELIM_LOCATION_PREFIX;
         }
         else if (val.equalsIgnoreCase("postfix"))
         {
            location = DELIM_LOCATION_POSTFIX;
         }
         else
         {
            throw new Exception("Property 'Location' must have value 'Infix', 'Prefix' or 'Postfix'.");
         }
         card.setDelimiterLocation(location);
      }

      card.setCharset(getProperty(properties, "Charset", false));
      card.setUseStrings(getBooleanProperty(properties, "UseStrings", false, false));

      // Get the Columns property
      boolean transferAll = true;
      String columns = properties.getProperty("Columns");
      if (columns != null)
      {
         if (columns.equalsIgnoreCase("Selected"))
         {
            transferAll = false;
         }
         else if (!columns.equalsIgnoreCase("All"))
         {
            throw new Exception("Property 'Columns' must have value 'All' or 'Selected'.");
         }
      }
      card.setTransferAll(transferAll);
   }


   // Replace special syntax characters with their character value
   private String substituteEscapes(String escape)
   {
      String literal = escape;

      // Special case for new line - get platform specific line separator
      if (escape.equals("[NL]") || escape.equals("<NL>"))
      {
         literal = System.getProperty("line.separator");
      }
      else
      {
         // Convert the literals to the character equivalent
         for (int i = 0;  i < _escapeLiterals.length;  i++)
         {
            if (_escapeLiterals[i][0].equals(escape))
            {
               literal = _escapeLiterals[i][1];
               break;
            }
         }
      }

      return literal;
   }


   private String getProperty(Properties props, String propertyName, boolean fRequired) throws Exception
   {
      String value = props.getProperty(propertyName);
      if (value != null)
      {
         value = value.trim();
         if (value.length() == 0)
         {
            value = null;
         }
      }

      if (value == null && fRequired)
      {
         throw new Exception ("Property \""+propertyName+"\" was not specified.");
      }
      return value;
   }


   private Integer getIntegerProperty(Properties props, String propertyName, boolean fRequired) throws Exception
   {
      Integer integerVal = null;
      String val = getProperty(props, propertyName, fRequired);
      if (val != null)
      {
         try
         {
            integerVal = new Integer(val);
         }
         catch (NumberFormatException nfe)
         {
            throw new Exception("Property \"" + propertyName + "\" must have an integral value");
         }
      }
      return integerVal;
   }


   private boolean getBooleanProperty(Properties props, 
             String propertyName,
             boolean fRequired,
             boolean fDefault) throws Exception
   {
      boolean boolVal;
      String val = getProperty(props, propertyName, fRequired);
      if (val == null)
      {
         boolVal = fDefault;
      }
      else
      {
         if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("on") || val.equals("1"))
         {
            boolVal = true;
         }
         else if (val.equalsIgnoreCase("false") || val.equalsIgnoreCase("off") || val.equals("0"))
         {
            boolVal = false;
         }
         else
         {
            throw new Exception("Property \"" + propertyName + "\" must have value \"true\" or \"false\"");
         }
      }
      return boolVal;
   }


   private static void logMessage(String message)
   {
      Logger.debug("--- " + message);
   }


   private static void logEntry()
   {
      Logger.debug("->> " + Thread.currentThread().getStackTrace()[3].toString());
   }


   private static void logExit()
   {
      Logger.debug("<<- " + Thread.currentThread().getStackTrace()[3].toString());
   }


   private static void logException(Throwable t)
   {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      t.printStackTrace(pw);
      pw.flush();
      sw.flush();
      Logger.information("Exception occurred: " + sw.toString());
   }


   // Subclass to provide direct access to the underlying buffer to avoid
   // a copy.  ByteArrayOutputStream.getByteArray makes a copy of the byte[]
   public class DirectByteArrayOutputStream extends ByteArrayOutputStream 
   {
      public DirectByteArrayOutputStream()
      {
         super();
      }
      
      public DirectByteArrayOutputStream(int size)
      {
         super(size);
      }
      
      public synchronized byte[] getByteArray()
      {
         return buf;
      }
      
      public synchronized int getCount()
      {
         return count;
      }
      
      public synchronized byte[] swap(byte[] newBuf)
      {
         byte[] oldBuf = buf;
         buf = newBuf;
         reset();
         return oldBuf;
      }
   }

   public class DirectByteArrayInputStream extends ByteArrayInputStream 
   {

       public DirectByteArrayInputStream(byte buf[])
       {
          super(buf);
       }
       
       public DirectByteArrayInputStream(byte buf[], int offset, int length)
       {
          super(buf, offset, length);
       }
       
       public synchronized void setByteArray(byte[] buf)
       {
           this.buf = buf;
           this.pos = 0;
           this.count = buf.length;
           this.mark = 0;
       }
       
       public synchronized void setByteArray(byte[] buf, int offset, int length)
       {
           this.buf = buf;
           this.pos = offset;
           this.count = Math.min( offset + length, buf.length );
           this.mark = offset;
       }
   }

   // Exception class that returns the position of the data
   public class ExhaustedDataException extends Exception
   {
      private int _position;

      public ExhaustedDataException(int position)
      {
         _position = position;
      }

      public int getPosition()
      {
         return _position;
      }
   }
}
