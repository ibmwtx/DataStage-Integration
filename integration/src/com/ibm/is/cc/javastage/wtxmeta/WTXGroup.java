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

public class WTXGroup
{
   private String       _name;
   private String       _path;
   private String       _delimiter;
   private String       _release;
   private String       _terminator;
   private String       _delimLocation;
   private boolean      _isDelimited;
   private List<String> _componentNames;
   private List<Object> _components;


   protected WTXGroup(String name, String path)
   {
      _name = name;
      _path = path;
      _isDelimited = false;
      _delimLocation = "Infix";
      _componentNames = new ArrayList<String>();
      _components = new ArrayList<Object>();
   }

   protected void setTerminator(String terminator)
   {
      _terminator = terminator;
   }

   protected void setDelimiter(String delimiter)
   {
      _delimiter = delimiter;
   }

   protected void setRelease(String release)
   {
      _release = release;
   }

   protected void setIsDelimited(boolean isDelimited)
   {
      _isDelimited = isDelimited;
   }

   protected void setDelimLocation(String delimLocation)
   {
      // Convert value to something that can be used in link property
      if (delimLocation.equalsIgnoreCase("INFIX"))
      {
         _delimLocation = "Infix";
      }
      else if (delimLocation.equalsIgnoreCase("PREFIX"))
      {
         _delimLocation = "Prefix";
      }
      else if (delimLocation.equalsIgnoreCase("POSTFIX"))
      {
         _delimLocation = "Postfix";
      }
   }

   protected void addComponentName(String path) throws Exception
   {
      _componentNames.add(path);

   }

   protected void stitchObjects(List<WTXItem> items, List<WTXGroup> groups) throws Exception
   {
      for (String path : _componentNames)
      {
         String itemName = path;
         String pathName = null;
         int idx = itemName.indexOf(' ');
         if (idx != -1)
         {
            pathName = itemName.substring(idx+1);
            itemName = itemName.substring(0, idx);
         }

         WTXItem item = null;
         for (WTXItem candidate : items)
         {
            if (candidate.getName().equals(itemName) &&
                (pathName == null || candidate.getPath().startsWith(pathName)))
            {
               item = candidate;
               break;
            }
         }

         if (item != null)
         {
            _components.add(item);
         }
         else
         {
            WTXGroup group = null;
            for (WTXGroup candidate : groups)
            {
               if (candidate.getName().equals(itemName) &&
                   (pathName == null || candidate.getPath().startsWith(pathName)))
               {
                  group = candidate;
                  break;
               }
            }

            if (group != null)
            {
               _components.add(group);
            }
            else
            {
               throw new Exception("Component '"+path+"' of group "+getName()+" could not be found.");
            }
         }
      }
   }

   protected String getName()
   {
      return _name;
   }

   protected String getPath()
   {
      return _path;
   }

   protected String getTerminator()
   {
      return _terminator;
   }

   protected String getDelimiter()
   {
      return _delimiter;
   }

   protected String getDelimLocation()
   {
      return _delimLocation;
   }

   protected String getRelease()
   {
      return _release;
   }

   protected boolean isDelimited()
   {
      return _isDelimited;
   }

   protected List<Object> getComponents()
   {
      return _components;
   }

   public String toString()
   {
      StringBuilder builder = new StringBuilder();
      builder.append("{"+_name);
      builder.append(", path="+_path);
      builder.append(", delimited="+_isDelimited);
      builder.append(", delimiter="+_delimiter);
      builder.append(", delimiter location="+_delimLocation);
      builder.append(", terminator="+_terminator);
      builder.append(", release="+_release);
      builder.append(", components={"+_components+"}");
      builder.append("}");
      return builder.toString();
   }
}



