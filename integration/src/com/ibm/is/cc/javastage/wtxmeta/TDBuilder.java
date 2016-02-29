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


public class TDBuilder
{
   private String _mtsFilename;
   private String _xmlFilename;
   private String _groupName;
   private String _groupPath;
   private String _tableDefName;


   public static void main(String[] args)
   {
      if (args.length < 4)
      {
         System.out.println("Usage:  TDBuilder <mts-file> <xml-file> <tabledef-name> <group-name> [<group-path>]");
         System.exit(1);
      }

      try
      {
         TDBuilder builder = new TDBuilder(args);
         builder.build();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(1);
      }
   }


   public TDBuilder(String[] args) throws Exception 
   {
      super();

      _mtsFilename  = args[0];
      _xmlFilename  = args[1];
      _tableDefName = args[2];
      _groupName    = args[3];

      // Process options
      _groupPath = null;
      if (args.length > 4)
      {
         _groupPath = args[4];
      }
   }


   public void build() throws Exception
   {
      // Parse the file
      MTSParser mtsParser = new MTSParser(_mtsFilename);
      mtsParser.parse();

      // Get the groups
      List<WTXGroup> groups = mtsParser.getGroups();

      // Create a new list with groups with matching name
      List<WTXGroup> matchingGroups = new ArrayList<WTXGroup>();
      for (WTXGroup group : groups)
      {
         if (group.getName().equals(_groupName))
         {
            matchingGroups.add(group);
         }
      }

      WTXGroup chosenGroup = null;
      if (matchingGroups.size() == 0)
      {
         throw new Exception("No group named '"+_groupName+"' could be found.");
      }
      else if (matchingGroups.size() > 1)
      {
         // There are more than one potential match - compare path
         if (_groupPath == null)
         {
            throw new Exception("There are "+matchingGroups.size()+" groups named '"+_groupName+"'. "+
                                "Specify the group path to distinguish.");
         }
      }
      else
      {
         // There is a single match
         chosenGroup = matchingGroups.get(0);
      }

      // Create ColumnDef objects that correspond to the TX items
      List<ColumnDef> columns = new ArrayList<ColumnDef>();

      processComponents("", columns, chosenGroup);

      // Finally build the file
      buildTableDef(chosenGroup, columns, mtsParser.getMTTFilename());
   }


   private void processComponents(String path, List<ColumnDef> columns, WTXGroup group)
   {
      for (Object item : group.getComponents())
      {
         if (!(item instanceof WTXItem))
         {
            WTXGroup groupComponent = (WTXGroup) item;
            System.out.println("Component '"+groupComponent.getName()+"' of '"+group.getName()+
                               "' is a group and will be flattened.");
            String stem = groupComponent.getName();
            if (path.length() > 0)
            {
               stem = path + "_"+ stem;
            }
            processComponents(stem, columns, groupComponent);
         }                    
         else
         {
            columns.add(createColumnDefFromItem(path, (WTXItem)item, group));
         }
      }
   }


   private ColumnDef createColumnDefFromItem(String path, WTXItem item, WTXGroup group)  
   {
      boolean isDelimited = group.isDelimited();
      ColumnDef column = new ColumnDef();

      String name = item.getName();
      if (path.length() > 0)
      {
         name = path + "_" + name;
      }
      column.setName(name);
      column.setIsNullable(true);

      switch (item.getType())
      {
      case WTXItem.CHAR:
         // If fixed format use fixed-width chars
         if (!isDelimited || (item.getMinLen() == item.getMaxLen()))
         {
            column.setSqlType(ColumnDef.SQL_TYPE_CHAR);
         }
         else
         {
            column.setSqlType(ColumnDef.SQL_TYPE_VARCHAR);
         }
         column.setPrecision(item.getMaxLen());
         break;

      case WTXItem.INTEGER:
         if (item.isBinary())
         {
            if (item.getSize() == 1)
            {
               column.setSqlType(ColumnDef.SQL_TYPE_TINYINT);
            }
            else if (item.getSize() == 2)
            {
               column.setSqlType(ColumnDef.SQL_TYPE_SMALLINT);
            }
            else if (item.getSize() == 4)
            {
               column.setSqlType(ColumnDef.SQL_TYPE_INTEGER);
            }
            else
            {
               column.setSqlType(ColumnDef.SQL_TYPE_BIGINT);
            }
         }
         else
         {
            column.setSqlType(ColumnDef.SQL_TYPE_INTEGER);
         }
         column.setIsSigned(item.isSigned());
         break;

      case WTXItem.BINARY:
         // If fixed format use fixed-width binaries
         if (!isDelimited || (item.getMinLen() == item.getMaxLen()))
         {
            column.setSqlType(ColumnDef.SQL_TYPE_BINARY);
         }
         else
         {
            column.setSqlType(ColumnDef.SQL_TYPE_VARBINARY);
         }
         column.setPrecision(item.getMaxLen());
         break;

      case WTXItem.FLOAT:
         if (item.getSize() == 4)
         {
            column.setSqlType(ColumnDef.SQL_TYPE_FLOAT);
         }
         else
         {
            column.setSqlType(ColumnDef.SQL_TYPE_DOUBLE);
         }
         break;

      case WTXItem.DECIMAL:
         column.setSqlType(ColumnDef.SQL_TYPE_DECIMAL);
         break;

      case WTXItem.TIMESTAMP:
         column.setSqlType(ColumnDef.SQL_TYPE_TIMESTAMP);
         column.setIsExtended(item.hasMicroseconds());
         break;

      case WTXItem.DATE:
         column.setSqlType(ColumnDef.SQL_TYPE_DATE);
         break;

      case WTXItem.TIME:
         column.setSqlType(ColumnDef.SQL_TYPE_TIME);
         column.setIsExtended(item.hasMicroseconds());
         break;
      }

      return column;
   }


   private void buildTableDef(WTXGroup group, List<ColumnDef> columns, String mttFilename) throws Exception
   {
      File xmlFile = new File(_xmlFilename);
      PrintWriter pw = new PrintWriter(xmlFile, "UTF-8");

      pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      pw.println("<DSExport>");
      pw.println("<Header CharacterSet=\"CP1252\" ExportingTool=\"IBM InfoSphere DataStage Export\" ToolVersion=\"8\" ServerVersion=\"9.1\"/>");
      pw.println("<TableDefinitions>");
      pw.println("<Record Identifier=\"None\\None\\"+_tableDefName+"\" Type=\"MetaTable\" Readonly=\"0\">");
      pw.println("<Property Name=\"Category\">\\Table Definitions</Property>");
      pw.println("<Property Name=\"Version\">9</Property>");

      pw.println("<Property Name=\"ShortDesc\">Generated from group "+group.getName()+" in type tree "+
                 escapeEntities(mttFilename)+"</Property>");
      pw.println("<Property Name=\"Description\" PreFormatted=\"1\">Use the following syntax objects in the link definition:");
      pw.println();
      if (group.isDelimited())
      {
         pw.println("Location="+group.getDelimLocation());
         pw.println("Delimiter="+escapeEntities(group.getDelimiter()));
      }
      pw.println("Terminator="+escapeEntities(group.getTerminator()));
      pw.println("ReleaseChar="+escapeEntities(group.getRelease()));
      pw.println("</Property>");

      pw.println("<Collection Name=\"Columns\" Type=\"MetaColumn\">");

      for (ColumnDef column : columns)
      {
         pw.println("<SubRecord>");
         pw.println("<Property Name=\"Name\">"+column.getName()+"</Property>");
         pw.println("<Property Name=\"SqlType\">"+column.getSqlType()+"</Property>");
         pw.println("<Property Name=\"Precision\">"+column.getPrecision()+"</Property>");
         pw.println("<Property Name=\"Scale\">"+column.getScale()+"</Property>");
         pw.println("<Property Name=\"Nullable\">"+boolToIntString(column.isNullable())+"</Property>");
         pw.println("<Property Name=\"KeyPosition\">0</Property>");
         pw.println("<Property Name=\"DisplaySize\">0</Property>");
         pw.println("<Property Name=\"LevelNo\">0</Property>");
         pw.println("<Property Name=\"Occurs\">0</Property>");
         pw.println("<Property Name=\"SignOption\">"+boolToIntString(column.isSigned())+"</Property>");
         pw.println("<Property Name=\"SCDPurpose\">0</Property>");
         pw.println("<Property Name=\"SyncIndicator\">0</Property>");
         pw.println("<Property Name=\"PadChar\"/>");
         pw.println("<Property Name=\"ExtendedPrecision\">0</Property>");
         pw.println("<Property Name=\"TaggedSubrec\">0</Property>");
         pw.println("<Property Name=\"OccursVarying\">0</Property>");
         pw.println("</SubRecord>");
      }

      pw.println("</Collection>");
      pw.println("</Record>");
      pw.println("</TableDefinitions>");
      pw.println("</DSExport>");

      pw.flush();
      pw.close();

      System.out.println("Created XML file "+xmlFile.getPath());
   }

   private static String boolToIntString(boolean val)
   {
      return val ? "1" : "0";
   }

   private static String escapeEntities(String val)
   {
      if (val == null)
      {
         return "";
      }

      String[][] substitutions =
      {
         {"&", "&amp;"},
         {"<", "&lt;"},
         {">", "&gt;"},
         {"\"", "&quot;"},
         {"'", "&apos;"},
      };

      for (int i = 0;  i < substitutions.length;  i++)
      {
         val = val.replaceAll(substitutions[i][0], substitutions[i][1]);
      }
      return val;
   }
}

