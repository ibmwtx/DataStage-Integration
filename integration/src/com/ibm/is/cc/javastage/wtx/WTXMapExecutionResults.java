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

import java.util.Iterator;
import java.util.LinkedList;

public class WTXMapExecutionResults
{
    private int    _mapInstance;
    private int    _returnCode;
    private String _returnMessage;

    public WTXMapExecutionResults()
    {
    }

    public void setMapInstance(int i)
    {
        _mapInstance = i;
    }

    public int getMapInstance()
    {
        return _mapInstance;
    }

    public void setResponseMessage(String s)
    {
        _returnMessage = s;
    }

    public String getResponseMessage()
    {
        return _returnMessage;
    }

    public void setResultCode(int i)
    {
        _returnCode = i;
    }

    public int getResultCode()
    {
        return _returnCode;
    }

    public boolean isError()
    {
        return !(isWarning() || isSuccess());
    }

    public boolean isWarning()
    {
        return (14 == _returnCode || 18 == _returnCode || 21 == _returnCode ||
                26 == _returnCode || 27 == _returnCode || 28 == _returnCode ||
                29 == _returnCode || 34 == _returnCode );
    }

    public boolean isSuccess()
    {
        return isSuccess(_returnCode);
    }

    public static boolean isSuccess(int rc)
    {
        return (0 == rc);
    }

    public String toString()
    {
        String s = "";
        s += "Map instance: " + _mapInstance + " ";
        s += "Return code: " + _returnCode + " ";
        s += "Message: " + _returnMessage + "\n";
        return s;
    }
}