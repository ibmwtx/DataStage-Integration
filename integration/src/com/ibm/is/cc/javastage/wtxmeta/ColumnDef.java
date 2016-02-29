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

import java.util.*;

public class ColumnDef
{
   private String  _name;
   private int     _sqlType;
   private int     _precision;
   private int     _scale;
   private int     _minLength;
   private int     _maxLength;
   private boolean _isNullable;
   private boolean _isSigned;
   private boolean _isExtended;

   // These constants are defined in the DataStage model
   public static final int SQL_TYPE_UNKNOWN       = 0;
   public static final int SQL_TYPE_CHAR          = 1;
   public static final int SQL_TYPE_NUMERIC       = 2;
   public static final int SQL_TYPE_DECIMAL       = 3;
   public static final int SQL_TYPE_INTEGER       = 4;
   public static final int SQL_TYPE_SMALLINT      = 5;
   public static final int SQL_TYPE_FLOAT         = 6;
   public static final int SQL_TYPE_REAL          = 7;
   public static final int SQL_TYPE_DOUBLE        = 8;
   public static final int SQL_TYPE_DATE          = 9;
   public static final int SQL_TYPE_TIME          = 10;
   public static final int SQL_TYPE_TIMESTAMP     = 11;
   public static final int SQL_TYPE_VARCHAR       = 12;
   public static final int SQL_TYPE_LONGVARCHAR   = -1;
   public static final int SQL_TYPE_BINARY        = -2;
   public static final int SQL_TYPE_VARBINARY     = -3;
   public static final int SQL_TYPE_LONGVARBINARY = -4;
   public static final int SQL_TYPE_BIGINT        = -5;
   public static final int SQL_TYPE_TINYINT       = -6;
   public static final int SQL_TYPE_BIT           = -7;
   public static final int SQL_TYPE_WCHAR         = -8;
   public static final int SQL_TYPE_WVARCHAR      = -9;
   public static final int SQL_TYPE_WLONGVARCHAR  = -10;
   public static final int SQL_TYPE_GUID          = -11;


   private static final Map<Integer, String> _dsTypeMap = new HashMap<Integer, String>()
   {
      {
         put(SQL_TYPE_UNKNOWN, "Unknown");
         put(SQL_TYPE_CHAR, "Char");
         put(SQL_TYPE_NUMERIC, "Numeric");
         put(SQL_TYPE_DECIMAL, "Decimal");
         put(SQL_TYPE_INTEGER, "Integer");
         put(SQL_TYPE_SMALLINT, "SmallInt");
         put(SQL_TYPE_FLOAT, "Float");
         put(SQL_TYPE_REAL, "Real");
         put(SQL_TYPE_DOUBLE, "Double");
         put(SQL_TYPE_DATE, "Date");
         put(SQL_TYPE_TIME, "Time");
         put(SQL_TYPE_TIMESTAMP, "Timestamp");
         put(SQL_TYPE_VARCHAR, "VarChar");
         put(SQL_TYPE_LONGVARCHAR, "LongVarChar");
         put(SQL_TYPE_BINARY, "Binary");
         put(SQL_TYPE_VARBINARY, "VarBinary");
         put(SQL_TYPE_LONGVARBINARY, "LongVarBinary");
         put(SQL_TYPE_BIGINT, "BigInt");
         put(SQL_TYPE_TINYINT, "TinyInt");
         put(SQL_TYPE_BIT, "Bit");
         put(SQL_TYPE_WCHAR, "NChar");
         put(SQL_TYPE_WVARCHAR, "NVarChar");
         put(SQL_TYPE_WLONGVARCHAR, "LongNVarChar");
         put(SQL_TYPE_GUID, "GUID");
      }
   };

   public ColumnDef()
   {
      _name = "";
      _sqlType = SQL_TYPE_UNKNOWN;
      _precision = 0;
      _scale = 0;
      _minLength = 0;
      _maxLength = 0;
      _isNullable = false;
      _isSigned = false;
      _isExtended = false;

      // Set lengths to default or fixed sizes
      setItemLengths();
   }

   public void setName(String name)
   {
      _name = name;
   }

   public void setSqlType(int sqlType)
   {
      _sqlType = sqlType;
   }

   public void setPrecision(int precision)
   {
      _precision = precision;
      setItemLengths();
   }

   public void setScale(int scale)
   {
      _scale = scale;
   }

   public void setIsNullable(boolean isNullable)
   {
      _isNullable = isNullable;
   }

   public void setIsSigned(boolean isSigned)
   {
      _isSigned = isSigned;
   }

   public void setIsExtended(boolean isExtended)
   {
      _isExtended = isExtended;
   }

   public String getName()
   {
      return _name;
   }

   public int getSqlType()
   {
      return _sqlType;
   }

   public int getPrecision()
   {
      return _precision;
   }

   public int getScale()
   {
      return _scale;
   }

   public boolean isNullable()
   {
      return _isNullable;
   }

   public boolean isSigned()
   {
      return _isSigned;
   }

   public boolean isExtended()
   {
      return _isExtended;
   }

   public String getSqlTypeAsString()
   {
      return _dsTypeMap.get(Integer.valueOf(_sqlType));
   }

   public int getMinLength()
   {
      return _minLength;
   }

   public int getMaxLength()
   {
      return _maxLength;
   }

   public String toString()
   {
      return getName() + 
             " (SqlType=" + getSqlTypeAsString() + ", " +
             "Precision=" + getPrecision() + ", " +
             "Scale=" + getScale() + ", " +
             "Nullable=" + isNullable() + ", " +
             "Signed=" + isSigned() + ", " +
             "Extended=" + isExtended() + ")";
   }

   private void setItemLengths()
   {
      switch(_sqlType)
      {
      case ColumnDef.SQL_TYPE_CHAR:
      case ColumnDef.SQL_TYPE_WCHAR:
         _minLength = _precision;
         _maxLength = _precision;
         break;

      case ColumnDef.SQL_TYPE_VARCHAR:
      case ColumnDef.SQL_TYPE_LONGVARCHAR:
      case ColumnDef.SQL_TYPE_WVARCHAR:
      case ColumnDef.SQL_TYPE_WLONGVARCHAR:
      case ColumnDef.SQL_TYPE_UNKNOWN:
         _minLength = 0;
         _maxLength = _precision;
         break;

      case ColumnDef.SQL_TYPE_NUMERIC:
      case ColumnDef.SQL_TYPE_DECIMAL:
         _minLength = 0;
         _maxLength = _precision;
         break;

      case ColumnDef.SQL_TYPE_INTEGER:
         _minLength = 8;
         _maxLength = 8;
         break;

      case ColumnDef.SQL_TYPE_SMALLINT:
      case ColumnDef.SQL_TYPE_BIT:
         _minLength = 4;
         _maxLength = 4;
         break;

      case ColumnDef.SQL_TYPE_TINYINT:
         _minLength = 2;
         _maxLength = 2;
         break;

      case ColumnDef.SQL_TYPE_FLOAT:
      case ColumnDef.SQL_TYPE_REAL:
         _minLength = 4;
         _maxLength = 4;
         break;

      case ColumnDef.SQL_TYPE_DOUBLE:
         _minLength = 8;
         _maxLength = 8;
         break;

      case ColumnDef.SQL_TYPE_BINARY:
         _minLength = _precision;
         _maxLength = _precision;
         break;

      case ColumnDef.SQL_TYPE_VARBINARY:
      case ColumnDef.SQL_TYPE_LONGVARBINARY:
         _minLength = 0;
         _maxLength = _precision;
         break;

      case ColumnDef.SQL_TYPE_BIGINT:
         _minLength = 0;
         _maxLength = 0;
         break;
      }
   }
}

