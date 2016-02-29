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

public class WTXItem
{
   protected final static int UNDEFINED = 0;
   protected final static int CHAR      = 1;
   protected final static int INTEGER   = 2;
   protected final static int BINARY    = 3;
   protected final static int FLOAT     = 4;
   protected final static int DECIMAL   = 5;
   protected final static int TIMESTAMP = 6;
   protected final static int DATE      = 7;
   protected final static int TIME      = 8;


   private String  _name;
   private String  _path;
   private boolean _isBinary = false;
   private boolean _isSigned = true;
   private boolean _hasMicroseconds = false;
   private int     _type = UNDEFINED;
   private int     _size = 0;
   private int     _minLen = 0;
   private int     _maxLen = 0;

   protected WTXItem(String name, String path)
   {
      _name = name;
      _path = path;
   }

   protected void setType(int type)
   {
      _type = type;
   }

   protected void setIsBinary(boolean isBinary)
   {
      _isBinary = isBinary;
   }

   protected void setIsSigned(boolean isSigned)
   {
      _isSigned = isSigned;
   }

   protected void hasMicroseconds(boolean hasMicroseconds)
   {
      _hasMicroseconds = hasMicroseconds;
   }

   protected void setSize(int size)
   {
      _size = size;
   }

   protected void setMinLen(int minLen)
   {
      _minLen = minLen;
   }

   protected void setMaxLen(int maxLen)
   {
      _maxLen = maxLen;
   }

   protected String getName()
   {
      return _name;
   }

   protected String getPath()
   {
      return _path;
   }

   protected int getType()
   {
      return _type;
   }

   protected int getSize()
   {
      return _size;
   }

   protected int getMinLen()
   {
      return _minLen;
   }

   protected int getMaxLen()
   {
      return _maxLen;
   }

   protected boolean isBinary()
   {
      return _isBinary;
   }

   protected boolean isSigned()
   {
      return _isSigned;
   }

   protected boolean hasMicroseconds()
   {
      return _hasMicroseconds;
   }


   public String toString()
   {
      StringBuilder builder = new StringBuilder();
      builder.append("{"+_name);
      builder.append(", path="+_path);
      builder.append(", type="+_type);
      builder.append(", isBinary="+_isBinary);
      builder.append(", size="+_size);
      builder.append(", min="+_minLen);
      builder.append(", max="+_maxLen);
      builder.append("}");
      return builder.toString();
   }
}



