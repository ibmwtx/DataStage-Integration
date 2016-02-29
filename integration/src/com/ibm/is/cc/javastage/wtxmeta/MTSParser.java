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


public class MTSParser extends DefaultHandler
{
   private final static int STATE_UNKNOWN          = 0;
   private final static int STATE_IN_ITEM          = 1;
   private final static int STATE_IN_GROUP         = 2;
   private final static int STATE_IN_TERMINATOR    = 3;
   private final static int STATE_IN_DELIMITER     = 4;
   private final static int STATE_IN_RELEASE       = 5;
   private final static int STATE_IN_SEQ_COMPONENT = 6;

   private int            _state;
   private List<WTXItem>  _items;
   private List<WTXGroup> _groups;
   private StringBuffer   _element;
   private String         _mtsFilename;
   private WTXItem        _currentItem;
   private WTXGroup       _currentGroup;
   private String         _mttFilename;


   public static void main(String[] args)
   {
      if (args.length < 1)
      {
         System.out.println("Usage:  MTSParser <mts-file>");
         System.exit(1);
      }

      try
      {
         MTSParser parser = new MTSParser(args[0]);
         parser.parse();

         System.out.println("ITEMS:");
         System.out.println(parser.getItems());
         System.out.println("GROUPS:");
         System.out.println(parser.getGroups());
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(1);
      }
   }


   public MTSParser(String mtsFilename) throws Exception 
   {
      super();

      _mtsFilename = mtsFilename;
   }


   public void parse() throws Exception
   {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      SAXParser saxParser = factory.newSAXParser();

      saxParser.getXMLReader().setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      saxParser.getXMLReader().setFeature("http://xml.org/sax/features/validation", false);

      // Initialize members
      _element = new StringBuffer();
      _items = new ArrayList<WTXItem>();
      _groups = new ArrayList<WTXGroup>();
      _currentItem = null;
      _currentGroup = null;
      _mttFilename = null;
      _state = STATE_UNKNOWN;

      // Parse the export file 
      saxParser.parse(new InputSource(new FileReader(new File(_mtsFilename))), 
                      (DefaultHandler) this);

      // Resolve the component names for groups
      for (WTXGroup group : _groups)
      {
         group.stitchObjects(_items, _groups);
         
      }
   }


   public List<WTXItem> getItems()
   {
      return _items;
   }


   public List<WTXGroup> getGroups()
   {
      return _groups;
   }


   public String getMTTFilename()
   {
      return _mttFilename;
   }


   public void startElement (String uri,
                             String localName,
                             String qName,
                             Attributes attrs) throws SAXException
   {
      if (_state == STATE_UNKNOWN)
      {
         if (qName.equals("ITEM"))
         {
            _currentItem = new WTXItem(attrs.getValue("SimpleTypeName"), attrs.getValue("CategoryOrItemParent"));
            _items.add(_currentItem);
            _state = STATE_IN_ITEM;
         }
         else if (qName.equals("GROUP"))
         {
            _currentGroup = new WTXGroup(attrs.getValue("SimpleTypeName"), attrs.getValue("CategoryOrGroupParent"));
            _groups.add(_currentGroup);
            _state = STATE_IN_GROUP;
         }
         else if (qName.equals("NEWTREE"))
         {
            _mttFilename = attrs.getValue("Filename");
         }
      }
      else
      {
         if (_state == STATE_IN_ITEM)
         {
            if (qName.startsWith("CharText"))  // startsWith because it may be CharTextWestern
            {
               _currentItem.setIsBinary(false);
               _currentItem.setType(WTXItem.CHAR);
            }
            else if (qName.equals("CharInt"))
            {
               _currentItem.setIsBinary(false);
               _currentItem.setType(WTXItem.INTEGER);
            }
            else if (qName.equals("BinaryText"))
            {
               _currentItem.setIsBinary(true);
               _currentItem.setType(WTXItem.BINARY);
            }
            else if (qName.equals("BinInt"))
            {
               _currentItem.setIsBinary(true);
               _currentItem.setType(WTXItem.INTEGER);
               _currentItem.setSize(toInt(attrs.getValue("Length")));
               String signed = attrs.getValue("Sign");
               if (signed != null)
               {
                  _currentItem.setIsSigned(signed.equals("YES"));
               }
            }
            else if (qName.equals("BinFloat"))
            {
               _currentItem.setIsBinary(true);
               _currentItem.setType(WTXItem.FLOAT);
               _currentItem.setSize(toInt(attrs.getValue("Length")));
            }
            else if (qName.equals("CharDateTimeFormatString"))
            {
               // Defer processing until we get the string format
            }
            else if (qName.equals("Decimal"))
            {
               _currentItem.setIsBinary(false);
               _currentItem.setType(WTXItem.DECIMAL);
            }
            else if (qName.equals("TotalDigits"))
            {
               _currentItem.setMinLen(toInt(attrs.getValue("Min")));
               _currentItem.setMaxLen(toInt(attrs.getValue("Max")));
            }

            // Sometimes Size is set, sometimes CharSize.
            else if (qName.equals("Size"))
            {
               _currentItem.setMinLen(toInt(attrs.getValue("Min")));
               int max = toInt(attrs.getValue("Max"));
               if (max != -1)
               {
                  _currentItem.setMaxLen(max);
               }
            }
            else if (qName.equals("CharSize"))
            {
               _currentItem.setMinLen(toInt(attrs.getValue("Min")));
               int max = toInt(attrs.getValue("Max"));
               if (max != -1)
               {
                  _currentItem.setMaxLen(max);
               }
            }
         }
         else if (_state == STATE_IN_GROUP)
         {
            if (qName.equals("TERMINATOR"))
            {
               _state = STATE_IN_TERMINATOR;
            }
            else if (qName.equals("RELEASE"))
            {
               _state = STATE_IN_RELEASE;
            }
            else if (qName.equals("DelimiterLiteral"))
            {
               _state = STATE_IN_DELIMITER;
            }
            else if (qName.equals("SequenceComponent"))
            {
               _state = STATE_IN_SEQ_COMPONENT;
            }
            else if (qName.equals("Delimited"))
            {
               _currentGroup.setIsDelimited(true);
               _currentGroup.setDelimLocation(attrs.getValue("location"));
            }
            else if (qName.equals("Fixed"))
            {
               _currentGroup.setIsDelimited(false);
            }
         }
      }

      _element.delete(0, _element.length());
   }


   public void endElement (String uri,
                           String localName,
                           String qName) throws SAXException
   {
      if (qName.equals("ITEM"))
      {
         _state = STATE_UNKNOWN;
      }
      else if (qName.equals("GROUP"))
      {
         _state = STATE_UNKNOWN;
      }

      else if (_state == STATE_IN_ITEM)
      {
         if (qName.equals("CharDateTimeFormatString"))
         {
            String format = _element.toString();
            if (format.indexOf("CCYY") != -1)
            {
               // It's a date or a timestamp - see if there is a time
               if (format.indexOf("HH24") != -1)
               {
                  _currentItem.setType(WTXItem.TIMESTAMP);
               }
               else
               {
                  _currentItem.setType(WTXItem.DATE);
               }
            }
            else
            {
               _currentItem.setType(WTXItem.TIME);
            }

            // See if it has microseconds
            if (format.indexOf("[.0-6]") != -1)
            {
               _currentItem.hasMicroseconds(true);
            }
         }
      }
      else if (_state == STATE_IN_TERMINATOR)
      {
         if (qName.equals("LiteralValue"))
         {
            _currentGroup.setTerminator(_element.toString());
            _state = STATE_IN_GROUP;
         }
      }
      else if (_state == STATE_IN_RELEASE)
      {
         if (qName.equals("LiteralValue"))
         {
            _currentGroup.setRelease(_element.toString());
            _state = STATE_IN_GROUP;
         }
      }
      else if (_state == STATE_IN_DELIMITER)
      {
         if (qName.equals("LiteralValue"))
         {
            _currentGroup.setDelimiter(_element.toString());
            _state = STATE_IN_GROUP;
         }
      }
      else if (_state == STATE_IN_SEQ_COMPONENT)
      {
         if (qName.equals("RelativeTypeName"))
         {
            try
            {
               _currentGroup.addComponentName(_element.toString());
            }
            catch (Exception e)
            {
              // throw new SAXException(e);
               System.out.println(e.getMessage());
            }
            _state = STATE_IN_GROUP;
         }
      }
   }


   public void characters(char[] ch, int start, int length)
   {
      _element.append(ch, start, length);
   }


   private int toInt(String val)
   {
      int ret;
      if (val.equals("S"))
      {
         ret = -1; 
      }
      else
      {
         ret = Integer.parseInt(val);
      }
      return ret;
   }


   private boolean toBoolean(String val)
   {
      return (val.equals("1") || val.equalsIgnoreCase("true"));
   }
}
