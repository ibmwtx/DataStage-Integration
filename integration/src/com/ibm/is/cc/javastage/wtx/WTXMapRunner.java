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

import com.ibm.websphere.dtx.dtxpi.MConstants;
import com.ibm.websphere.dtx.dtxpi.MException;
import com.ibm.websphere.dtx.dtxpi.MAdapter;
import com.ibm.websphere.dtx.dtxpi.MCard;
import com.ibm.websphere.dtx.dtxpi.MMap;
import com.ibm.websphere.dtx.dtxpi.MStream;
import com.ibm.websphere.dtx.dtxpi.tools.MJniUtils;

import com.ibm.is.cc.javastage.api.*;

import java.io.*;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.Hashtable;


// Class that interfaces with the Websphere TX Java API (dtxpi)
public class WTXMapRunner
{
   private Hashtable<Integer,WTXInputCard> _inputCardData = null;
   private Hashtable<Integer,WTXOutputCard> _outputCardData = null;

   private static boolean _apiInitialized = false;  
   private static int     _mapInstance = 0;
   private MMap           _map;
   private MMap           _cacheMap;
   private String         _mapName;
   private String         _mapFile;
   private String         _logDirectory;
   private Boolean        _fMapAudit;
   private Boolean        _fMapTrace;


   public WTXMapRunner(String  logDirectory,
                       String  resourceFileName,
                       Boolean fMapTrace,
                       Boolean fMapAudit) throws Exception
   {
      if (isApiInitialized() == false)
      {
         initializeAPI(resourceFileName);
         ClassLoader cl = this.getClass().getClassLoader();
         Class partypes[] = new Class[]{(new String()).getClass()};
         Method methodID = cl.getClass().getMethod("loadClass", partypes);
         MJniUtils.setCustomClassLoader(methodID, cl, null); 

         _logDirectory = logDirectory;
         _fMapAudit = fMapAudit;
         _fMapTrace = fMapTrace;
      }
   }


   private void initializeAPI(String resourceFileName) throws Exception
   {
      if (resourceFileName != null && resourceFileName.length() == 0)
      {
         resourceFileName = null;
      }
      MMap.initializeAPI(resourceFileName);
      _apiInitialized = true;
   }


   public void close()
   {
      try
      {
         MMap.terminateAPI();
      }
      catch (MException me)
      {
         ;
      }
   }


   public void loadMap(String mapFile) throws Exception
   {  
      // Open the map file
      FileInputStream fis = null;
      try
      {
         fis = new FileInputStream(mapFile);
      }
      catch (FileNotFoundException e)
      {
         throw new Exception("Map file "+mapFile+" could not be opened.");
      }
      byte[] mapBytes = new byte[fis.available()];
      fis.read(mapBytes);
      fis.close();

      _mapFile = mapFile;
      _mapName = getMapName(mapFile);

      _cacheMap = new MMap(_mapName, _mapFile, mapBytes);

      Logger.information("Loaded map file "+mapFile);
   }


   public void unload() throws Exception
   {
      if (_cacheMap != null)
      {
         _cacheMap.unload();   
      }
   }


   public void overrideInput(WTXInputCard override)
   {
      if (_inputCardData == null)
      {
         _inputCardData = new Hashtable<Integer,WTXInputCard>();
      }
      _inputCardData.put(new Integer(override.getCardNumber()), override);
   }


   public void overrideOutput(WTXOutputCard override)
   {
      if (_outputCardData == null)
      {
         _outputCardData = new Hashtable<Integer,WTXOutputCard>();
      }
      _outputCardData.put(new Integer(override.getCardNumber()), override);
   }


   public WTXMapExecutionResults executeMap() throws Exception
   {
      WTXMapExecutionResults executionResults = new WTXMapExecutionResults(); 

      // load the map from the cache
      _map = new MMap(_mapName, _mapFile, null);
      _mapInstance++;

      // Set the map instance manually - since we will need it before the map runs (for the ObjectPool)
      _map.setIntegerProperty(MConstants.MPIP_MAP_INSTANCE, 0, _mapInstance);
      _map.setIntegerProperty(MConstants.MPIP_MAP_USE_RESOURCE_MANAGER, 0, 1);

      // Set up the trace and audit directories 
      setupTraceAndAuditDirs();

      // Override inputs
      if (_inputCardData != null)
      {
         for (Integer cardNum : _inputCardData.keySet())
         {
            MCard inCard = null;
            try
            {
               inCard = _map.getInputCardObject(cardNum);
            }
            catch (MException me)
            {
               throw new Exception("Input card number "+cardNum+" is invalid for map "+_mapName);
            }
            inCard.overrideAdapter(null, MConstants.MPI_ADAPTYPE_STREAM);

            // Pass the data to the card
            WTXInputCard override = _inputCardData.get(cardNum);
            MAdapter adapter = inCard.getAdapter();
            MStream stream = adapter.getOutputStream();
            stream.write(override.getInputData(), 0, override.getInputDataCount());
         }
      }

      // Override outputs to streams
      if (_outputCardData != null)
      {
         for (Integer cardNum : _outputCardData.keySet())
         {
            MCard outCard = null;
            try
            {
               outCard = _map.getOutputCardObject(cardNum);
            }
            catch (MException me)
            {
               throw new Exception("Output card number "+cardNum+" is invalid for map "+_mapName);
            }
            outCard.overrideAdapter(null, MConstants.MPI_ADAPTYPE_STREAM);
         }
      }

      // Run the map
      _map.run();

      // Populate the output streams
      if (_outputCardData != null)
      {
         for (Integer cardNum : _outputCardData.keySet())
         {
            // Get the output stream
            WTXOutputCard override = _outputCardData.get(cardNum);
            OutputStream outputStream = override.getOutputStream();

            MCard outCard = _map.getOutputCardObject(cardNum);
            MAdapter adapter = outCard.getAdapter();
            MStream stream = adapter.getInputStream();

            // Get the data in pieces from the stream 
            stream.seek(0, MConstants.MPI_SEEK_SET);
            while (true)
            {
               if (stream.isEnd())
               {
                  break;
               }

               outputStream.write(stream.readPage());
            }
            stream.seek(0, MConstants.MPI_SEEK_SET);
         }
      }

      // Gather the run results
      executionResults.setMapInstance(_map.getIntegerProperty(MConstants.MPIP_MAP_INSTANCE, 0)); 
      executionResults.setResponseMessage(_map.getTextProperty(MConstants.MPIP_OBJECT_ERROR_MSG, 0));
      executionResults.setResultCode(_map.getIntegerProperty(MConstants.MPIP_OBJECT_ERROR_CODE, 0));

      // Unload the map instance
      _map.unload();

      return executionResults;
   }

   private String getMapName(String filename)
   {
      String mapName = filename;

      // File separators could be either forward or backwards slash (both are accepted on Windows)
      // so look for both.
      int idxSlash = mapName.lastIndexOf("/");
      if (idxSlash != -1)
      {
         mapName = mapName.substring(idxSlash+1);
      }
      else
      {
         idxSlash = mapName.lastIndexOf("\\");
         if (idxSlash != -1)
         {
            mapName = mapName.substring(idxSlash+1);
         }
      }

      int idxExtension = mapName.lastIndexOf('.');
      if (idxExtension != -1)
      {
         mapName = mapName.substring(0, idxExtension);
      }

      return mapName;
   }

   private boolean isApiInitialized()
   {
      return _apiInitialized;
   }

   private void setupTraceAndAuditDirs() throws Exception
   {   
      if (_fMapTrace != null)
      {
         // Turn trace on or off and set directory to log directory
         _map.setIntegerProperty(MConstants.MPIP_MAP_TRACE_SWITCH, 0, _fMapTrace ? 1 : 0);

         if (_logDirectory != null)
         {
            _map.setIntegerProperty(MConstants.MPIP_MAP_TRACE_DIRECTORY, 0,
                                    MConstants.MPI_DIRECTORY_CUSTOM);
            _map.setTextProperty(MConstants.MPIP_MAP_TRACE_DIRECTORY_CUSTOM_VALUE,
                                 0, _logDirectory);
         }
      }

      if (_fMapAudit != null)
      {
         // Turn audit on or off and set directory to log directory
         _map.setIntegerProperty(MConstants.MPIP_MAP_AUDIT_SWITCH, 0, _fMapAudit ? 1 : 0);

         if (_logDirectory != null)
         {
            _map.setIntegerProperty(MConstants.MPIP_MAP_AUDIT_LOCATION, 0,
                                    MConstants.MPI_LOCATION_FILE);
            _map.setIntegerProperty(MConstants.MPIP_MAP_AUDIT_DIRECTORY, 0,
                                    MConstants.MPI_DIRECTORY_CUSTOM);
            _map.setTextProperty(MConstants.MPIP_MAP_AUDIT_DIRECTORY_CUSTOM_VALUE,
                                 0, _logDirectory);
         }
      }
   }
}

