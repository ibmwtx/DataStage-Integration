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

package com.ibm.is.cc.javastage.wtxmeta;

import java.io.*;
import java.util.*;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.*;


public class MTSBuilder extends DefaultHandler
{
   private StringBuffer    _element;
   private String          _tableDefFilename;
   private String          _mtsFilename;
   private String          _mttFilename;
   private String          _language;
   private String          _charset;
   private boolean         _asStrings = false;
   private boolean         _foundCollection;
   private boolean         _foundColumn;
   private List<ColumnDef> _columns;
   private ColumnDef       _currentColumn;
   private String          _currentProperty;


   public static void main(String[] args)
   {
      if (args.length < 3)
      {
         System.out.println("Usage:  MTSBuilder <xml-export-file> <mts-file> <mtt-file> "+
                            "[-L <language>] [-C <charset>] [-S]");
         System.exit(1);
      }

      try
      {
         MTSBuilder builder = new MTSBuilder(args);
         builder.build();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(1);
      }
   }


   public MTSBuilder(String[] args) throws Exception 
   {
      super();

      _tableDefFilename = args[0];
      _mtsFilename = args[1];
      _mttFilename = args[2];

      // Process options
      if (args.length > 3)
      {
         _language = getOption("-L", args, 3, true);
         _charset = getOption("-C", args, 3, true);
         _asStrings = getOption("-S", args, 3, false) != null;
      }
      if (_language == null)
      {
         _language = "Western";
      }
      if (_charset == null)
      {
         _charset = "NATIVE";
      }
   }


   public void build() throws Exception
   {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      SAXParser saxParser = factory.newSAXParser();

      // Initialize members
      _element = new StringBuffer();
      _foundCollection = false;
      _foundColumn = false;
      _columns = new ArrayList<ColumnDef>();
      _currentColumn = null;
      _currentProperty = null;

      // Parse the export file 
      saxParser.parse(new InputSource(new FileReader(new File(_tableDefFilename))), 
                      (DefaultHandler) this);

      buildMTS();
   }


   public void startElement (String uri,
                             String localName,
                             String qName,
                             Attributes attrs) throws SAXException
   {
      if (qName.equals("Collection"))
      {
         _foundCollection = true;
      }
      else if (qName.equals("SubRecord") && _foundCollection)
      {
         _currentColumn = new ColumnDef();
         _columns.add(_currentColumn);
         _foundColumn = true;
      }
      else if (qName.equals("Property") && _foundColumn)
      {
         _currentProperty = attrs.getValue("Name");
      }

      _element.delete(0, _element.length());
   }


   public void endElement (String uri,
                           String localName,
                           String qName) throws SAXException
   {
      if (qName.equals("SubRecord"))
      {
         _foundColumn = false;
      }
      else if (qName.equals("Collection"))
      {
         _foundCollection = false;
      }
      else if (qName.equals("Property") && _foundColumn)
      {
         if (_currentProperty.equals("Name"))
         {
            _currentColumn.setName(_element.toString());
         }
         else if (_currentProperty.equals("SqlType"))
         {
            _currentColumn.setSqlType(toInt(_element.toString()));
         }
         else if (_currentProperty.equals("Precision"))
         {
            _currentColumn.setPrecision(toInt(_element.toString()));
         }
         else if (_currentProperty.equals("Scale"))
         {
            _currentColumn.setScale(toInt(_element.toString()));
         }
         else if (_currentProperty.equals("Nullable"))
         {
            _currentColumn.setIsNullable(toBoolean(_element.toString()));
         }
         else if (_currentProperty.equals("SignOption"))
         {
            _currentColumn.setIsSigned(toBoolean(_element.toString()));
         }
         else if (_currentProperty.equals("ExtendedPrecision"))
         {
            _currentColumn.setIsExtended(toBoolean(_element.toString()));
         }
      }
   }


   public void characters(char[] ch, int start, int length)
   {
      _element.append(ch, start, length);
   }


   private int toInt(String val)
   {
      return Integer.parseInt(val);
   }


   private boolean toBoolean(String val)
   {
      return (val.equals("1") || val.equalsIgnoreCase("true"));
   }



   private String getOption(String option, String[] args, int index, boolean fHasValue) throws Exception
   {
      String ret = null;

      for (int i = index;  i < args.length;  i++)
      {
         String first = args[i];
         String second = null;
         if (i + 1 <= args.length - 1)
         {
            second = args[i+1];
         }

         if (first.equals(option))
         {
            if (fHasValue)
            {
               // second must be the value
               if (second == null)
               {
                  throw new Exception("No value was provided for option "+option);
               }
               ret = second;
            }
            else
            {
               // No value required - return "1" to indicate option provided
               ret = "1";
            }
            break;
         }
         else if (first.startsWith(option))
         {
            ret = first.substring(option.length());
            break;
         }
      }

      return ret;
   }


   private void buildMTS() throws Exception
   {
      File mtsFile = new File(_mtsFilename);
      PrintWriter pw = new PrintWriter(mtsFile, "UTF-8");

      // Create the root object
      pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      pw.println("<!DOCTYPE TTMAKER SYSTEM \"ttmaker60.dtd\">");
      pw.println("<?ANALYZE?><TTMAKER Version=\"6.0\"><NEWTREE Filename=\""+_mttFilename+"\">");
      pw.println("<ROOT SimpleTypeName=\"Root\" SetUpProperties=\"DEFAULT\" SetUpComponents=\"DELETE\" OrderSubtypes=\"ASCENDING\">");
      pw.println("<Sequence partition=\"NO\"></Sequence>");
      pw.println("<CharText"+_language+"><Size Min=\"0\" Max=\"S\"/>");
      pw.println("<CharSize Min=\"0\" Max=\"S\"/>");
      pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
      pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
      pw.println("</CharText"+_language+">");
      pw.println("</ROOT>");

      for (ColumnDef column : _columns)
      {
         buildItem(pw, column);
      }

      // Create the document group
      pw.println("<GROUP SimpleTypeName=\"Document\" CategoryOrGroupParent=\"Root\" OrderSubtypes=\"ASCENDING\">");
      pw.println("<Sequence partition=\"NO\"><SequenceComponent><RelativeTypeName>Row</RelativeTypeName>");
      pw.println("<Range Min=\"0\" Max=\"S\"/>");
      pw.println("</SequenceComponent>");
      pw.println("</Sequence>");
      pw.println("</GROUP>");

      // Create the row group
      pw.println("<GROUP SimpleTypeName=\"Row\" CategoryOrGroupParent=\"Root\" OrderSubtypes=\"ASCENDING\">");
      pw.println("<TypeSyntax><TERMINATOR><Literal IgnoreCase=\"NO\">");
      pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
      pw.println("<LiteralValue>&lt;NL&gt;</LiteralValue>");
      pw.println("</Literal>");
      pw.println("</TERMINATOR>");
      pw.println("<RELEASE><OneByteLiteral><LiteralValue>!</LiteralValue>");
      pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
      pw.println("</OneByteLiteral>");
      pw.println("</RELEASE>");
      pw.println("</TypeSyntax>");
      pw.println("<Sequence partition=\"NO\"><Implicit><Delimited location=\"INFIX\"><DelimiterLiteral>");
      pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
      pw.println("<LiteralValue>|</LiteralValue>");
      pw.println("</DelimiterLiteral>");
      pw.println("</Delimited>");
      pw.println("</Implicit>");

      // Add the row components
      int numSizeOfs = 0;
      for (ColumnDef column : _columns)
      {
         // If a varbinary type, add a sized component
         if (!_asStrings && 
             (column.getSqlType() == ColumnDef.SQL_TYPE_VARBINARY ||
              column.getSqlType() == ColumnDef.SQL_TYPE_LONGVARBINARY))
         {
            numSizeOfs++;
            pw.println("<SequenceComponent SIZED=\"EXCLUDESELF\"><RelativeTypeName>sizeof"+numSizeOfs+"</RelativeTypeName>");
            pw.println("<Range Min=\"1\" Max=\"1\"/>");
            pw.println("</SequenceComponent>");
         }

         pw.println("<SequenceComponent><RelativeTypeName>"+column.getName()+"</RelativeTypeName>");
         pw.println("<Range Min=\"1\" Max=\"1\"/>");
         pw.println("</SequenceComponent>");
      }

      // Complete the Row group definition
      pw.println("</Sequence>");
      pw.println("</GROUP>");

      // Add sizeof items
      for (int i = 1;  i <= numSizeOfs;  i++)
      {
         pw.println("<ITEM SimpleTypeName=\"sizeof"+i+"\" CategoryOrItemParent=\"Root\" partition=\"NO\" OrderSubtypes=\"ASCENDING\">");
         pw.println("<BinaryNumber><BinInt Length=\"4\" Sign=\"NO\" ByteOrder=\"NATIVE\"/>");
         pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
         pw.println("</BinaryNumber>");
         pw.println("</ITEM>");
      }

      pw.println("</NEWTREE>");
      pw.println("</TTMAKER>");

      pw.flush();
      pw.close();

      System.out.println("Created MTS script "+mtsFile.getPath());
   }


   private void buildItem(PrintWriter pw, ColumnDef column)
   {
      pw.println("<ITEM SimpleTypeName=\""+column.getName()+"\" CategoryOrItemParent=\"Root\" partition=\"NO\" OrderSubtypes=\"ASCENDING\">");

      // Set a string for maxLength - either the integer size or "S"
      String maxLength = "S";
      if (column.getMaxLength() != 0)
      {
         maxLength = Integer.toString(column.getMaxLength());
      }

      switch(column.getSqlType())
      {
      case ColumnDef.SQL_TYPE_CHAR:
      case ColumnDef.SQL_TYPE_VARCHAR:
      case ColumnDef.SQL_TYPE_UNKNOWN:
      case ColumnDef.SQL_TYPE_WCHAR:
      case ColumnDef.SQL_TYPE_WVARCHAR:
      case ColumnDef.SQL_TYPE_WLONGVARCHAR:
      case ColumnDef.SQL_TYPE_GUID:
      case ColumnDef.SQL_TYPE_LONGVARCHAR:
         pw.println("<CharText"+_language+"><Size Min=\"0\" Max=\"S\"/>");
         pw.println("<CharSize Min=\""+column.getMinLength()+"\" Max=\""+maxLength+"\"/>");
         pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
         pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
         pw.println("</CharText"+_language+">");
         break;

      case ColumnDef.SQL_TYPE_NUMERIC:
      case ColumnDef.SQL_TYPE_DECIMAL:
         pw.println("<CharNumber><Decimal><TotalDigits Min=\"0\" Max=\"S\"/>");
         pw.println("<DecimalFormatString>{####[&apos;.&apos;]##}</DecimalFormatString>");
         pw.println("</Decimal>");
         pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
         pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
         pw.println("</CharNumber>");
         break;

      case ColumnDef.SQL_TYPE_INTEGER:
      case ColumnDef.SQL_TYPE_SMALLINT:
      case ColumnDef.SQL_TYPE_TINYINT:
      case ColumnDef.SQL_TYPE_BIT:
         if (!_asStrings)
         {
            pw.println("<BinaryNumber><BinInt Length=\""+maxLength+"\" Sign=\"YES\" ByteOrder=\"NATIVE\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</BinaryNumber>");
         }
         else
         {
            pw.println("<CharNumber><CharInt><TotalDigits Min=\"0\" Max=\"S\"/>");
            pw.println("<IntegerFormatString>{######}</IntegerFormatString>");
            pw.println("</CharInt>");
            pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</CharNumber>");
         }
         break;

      case ColumnDef.SQL_TYPE_FLOAT:
      case ColumnDef.SQL_TYPE_REAL:
      case ColumnDef.SQL_TYPE_DOUBLE:
         if (!_asStrings)
         {
            pw.println("<BinaryNumber><BinFloat Length=\""+maxLength+"\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</BinaryNumber>");
         }
         else
         {
            pw.println("<CharNumber><Decimal><TotalDigits Min=\"0\" Max=\"S\"/>");
            pw.println("<DecimalFormatString>{####[&apos;.&apos;]##}</DecimalFormatString>");
            pw.println("</Decimal>");
            pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</CharNumber>");
         }
         break;

      case ColumnDef.SQL_TYPE_DATE:
      case ColumnDef.SQL_TYPE_TIME:
      case ColumnDef.SQL_TYPE_TIMESTAMP:
         {
            String format = null;
            // isExtended indictes that microseconds are selected
            if (column.getSqlType() == ColumnDef.SQL_TYPE_DATE)
            {
               format = "{CCYY-MM-DD}";
            }
            else if (column.getSqlType() == ColumnDef.SQL_TYPE_TIME)
            {
               format = column.isExtended() ? "{HH24:MM:SS[.0-6]}" : "{HH24:MM:SS}";
            }
            else
            {
               format = column.isExtended() ? "{CCYY-MM-DD} {HH24:MM:SS[.0-6]}" : "{CCYY-MM-DD} {HH24:MM:SS}";
            }
            pw.println("<CharDatetime><CharDateTimeFormatString>"+format+"</CharDateTimeFormatString>");
            pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</CharDatetime>");
         }
         break;

      case ColumnDef.SQL_TYPE_BINARY:
      case ColumnDef.SQL_TYPE_VARBINARY:
      case ColumnDef.SQL_TYPE_LONGVARBINARY:
         if (!_asStrings)
         {
            pw.println("<BinaryText><Size Min=\""+column.getMinLength()+"\" Max=\""+maxLength+"\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</BinaryText>");
         }
         else
         {  
            pw.println("<CharText"+_language+"><Size Min=\"0\" Max=\"S\"/>");
            pw.println("<CharSize Min=\""+column.getMinLength()*2+"\" Max=\""+column.getMaxLength()*2+"\"/>");
            pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
            pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
            pw.println("</CharText"+_language+">");
         }
         break;

      case ColumnDef.SQL_TYPE_BIGINT:
         pw.println("<CharNumber><CharInt><TotalDigits Min=\"0\" Max=\"S\"/>");
         pw.println("<IntegerFormatString>{######}</IntegerFormatString>");
         pw.println("</CharInt>");
         pw.println("<"+_language+" CharSet=\""+_charset+"\"/>");
         pw.println("<ValueRestrictions IgnoreCase=\"NO\" Rule=\"INCLUDE\"></ValueRestrictions>");
         pw.println("</CharNumber>");
         break;
      }

      pw.println("</ITEM>");
   }
}
