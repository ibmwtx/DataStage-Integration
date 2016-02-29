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

public class WTXInputCard extends WTXCard
{
   // For input links
   private byte[]  _inputData;
   private int     _byteCount;

   public WTXInputCard(int linkNum, int cardNum)
   {
      _linkNumber = linkNum;
      _cardNumber = cardNum;
   }

   public void setInputData(byte[] data, int count)
   {
      _inputData = data;
      _byteCount = count;
   }

   public byte[] getInputData()
   {
      return _inputData;
   }

   public int getInputDataCount()
   {
      return _byteCount;
   }
}
