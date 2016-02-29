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

public abstract class WTXCard
{
   protected int     _cardNumber;
   protected int     _linkNumber;
   protected String  _delimiter = null;
   protected String  _terminator = null;
   protected String  _releaseChar = null;
   protected String  _charset = null;
   protected boolean _useStrings = false;
   protected boolean _transferAll = true;
   protected int     _location;


   public void setDelimiter(String delimiter)
   {
      _delimiter = delimiter;
   }

   public void setTerminator(String terminator)
   {
      _terminator = terminator;
   }

   public void setReleaseChar(String release)
   {
      _releaseChar = release;
   }

   public void setUseStrings(boolean useStrings)
   {
      _useStrings = useStrings;
   }

   public void setTransferAll(boolean transferAll)
   {
      _transferAll = transferAll;
   }

   public void setCharset(String charset)
   {
      _charset = charset;
   }

   public void setDelimiterLocation(int location)
   {
      _location = location;
   }

   public int getCardNumber()
   {
      return _cardNumber;
   }

   public int getLinkNumber()
   {
      return _linkNumber;
   }

   public String getDelimiter()
   {
      return _delimiter;
   }

   public String getTerminator()
   {
      return _terminator;
   }

   public String getReleaseChar()
   {
      return _releaseChar;
   }

   public boolean useStrings()
   {
      return _useStrings;
   }

   public boolean transferAll()
   {
      return _transferAll;
   }

   public String getCharset()
   {
      return _charset;
   }

   public int getDelimiterLocation()
   {
      return _location;
   }
}
