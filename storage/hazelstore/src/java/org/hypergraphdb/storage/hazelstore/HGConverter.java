package org.hypergraphdb.storage.hazelstore;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class HGConverter
{
    public static int handleSize = 16; 

	public static byte [] convertStringtoByteArray( String s)
	{
	try 						
	{	
		// return IOUtils.toByteArray(new StringReader(s), );	
		return s.getBytes();
	}
	catch (Exception ioEx) 	{throw new HGException("Error when converting String to Byte" + ioEx);}
	}
	
    
    public static String convertByteArraytoString(byte [] b)
    {
    	try 						
    	{
    		return new String(b);
    		// return IOUtils.toString(new ByteArrayInputStream(b), );	
    	}

    	catch (Exception ioEx) 	{throw new HGException("Error when converting from Byte to String " + ioEx);}
	}
    
	
    public static String convertHandleArrayToString ( HGPersistentHandle[] link, int handleSize)
	    {
		  String resultstring = null;
		  try {
	    		byte[] buffer = new byte[link.length * handleSize];
	    		for (int i = 0; i < link.length; i++)
	           {
	               HGPersistentHandle handle = (HGPersistentHandle)link[i];
	               System.arraycopy(handle.toByteArray(), 0, 
	                                buffer, i*handleSize, 
	                                handleSize);             
				}
	    		resultstring = new String(buffer);
			} catch (Exception e) {
				e.printStackTrace();
			}
    		return resultstring;
	    }   

	public static HGPersistentHandle[] convertStringToHandleArray( String inputString, int handlesize,  HGHandleFactory hghandlefactory)
	    {
		  HGPersistentHandle[] handles;
		  
		  byte[] input = HGConverter.convertStringtoByteArray(inputString);
		  int size = input.length;
		  
	    		if (input == null)
	    		  	return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;
	    		else if (size == 0)
	   		   		return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;
	    		else if (size % handleSize != 0)
	    			throw new HGException("While reading link tuple: the value buffer size is not a multiple of the handle size.");
	    		else 
	    			{ 	int handle_count = size / handleSize;
	    			  	handles = new HGPersistentHandle[handle_count];
	    			  	for (int i = 0; i < handle_count; i++)  
	    			  		handles[i] = hghandlefactory.makeHandle(input, i*handleSize);  // in LinkBinding.readHandle calls is called makeHandle with buffer, "offset + i*handleSize", whereas offset comes from entryToObject which is just 0
	    			}  	
	    		
	    		return handles;
	    }
	  	
	
    public static byte[] convertHandleArrayToByteArray( HGPersistentHandle[] link, int handleSize)
  {    	
  		byte [] buffer = new byte[link.length * handleSize];
  		for (int i = 0; i < link.length; i++)
         {
             HGPersistentHandle handle = (HGPersistentHandle)link[i];
             System.arraycopy(handle.toByteArray(), 0, 
                              buffer, i*handleSize, 
                              handleSize);            
         }
      return buffer;
  }   

	
    public static HGPersistentHandle[] convertByteArrayToHandleArray( byte[] input,  HGHandleFactory hghandlefactory)
	  {
        if (input == null)
	  		  	return null;
          int size = input.length;



          if (size == 0)
                return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;

          if (size % handleSize != 0)
            throw new HGException("While reading link tuple: the value buffer size is not a multiple of the handle size.");
	     int handle_count = size / handleSize;
	 	 HGPersistentHandle[] handles = new HGPersistentHandle[handle_count];
	         for (int i = 0; i < handle_count; i++)  
	             handles[i] = hghandlefactory.makeHandle(input, i*handleSize);  // in LinkBinding.readHandle calls is called makeHandle with buffer, "offset + i*handleSize", whereas offset comes from entryToObject which is just 0 ?
	     return handles;
	  }

	
	
    public static HGPersistentHandle[] convertByteArrayToHandleArray( byte[] input, int handlesize,  HGHandleFactory hghandlefactory)
  {
	  int size = input.length;
	  
  		if (input == null)
  		  	return null;
  	  
  	  if (size == 0)
 		   	return HyperGraph.EMPTY_PERSISTENT_HANDLE_SET;

  	  if (size % handleSize != 0)
        throw new HGException("While reading link tuple: the value buffer size is not a multiple of the handle size.");
     int handle_count = size / handleSize;
 	   HGPersistentHandle[] handles = new HGPersistentHandle[handle_count];
         for (int i = 0; i < handle_count; i++)  
             handles[i] = hghandlefactory.makeHandle(input, i*handleSize);  // in LinkBinding.readHandle calls is called makeHandle with buffer, "offset + i*handleSize", whereas offset comes from entryToObject which is just 0
     return handles;
  }
  
	
    public static HGPersistentHandle[]  convertBAsetToHandleArray( Set<byte[]> in,  HGHandleFactory hghandlefactory)
	{
		return convertByteArrayToHandleArray(flattenBASetToBA(in), hghandlefactory);
	}
	
    public static byte[] flattenBASetToBA ( Set<byte[]> in)
	{	
		int index = 0;
		byte[] resultBA = new byte[in.size()*16];   // TODO - generalize handleSize = handleFactory.nullHandle().toByteArray().length;
				 
		Iterator<byte[]> it = in.iterator();
		while(it.hasNext())
		{
			byte[] current = it.next();
			for(int i = 0; i<current.length-1; i++)
			{ 
				resultBA[index]= current[i];
				index ++;
			}
		}
		return resultBA;
	}

    // next two methods taken from: http://stackoverflow.com/questions/5399798/byte-array-and-int-conversion-in-java/5399829#5399829

    
    public static Integer byteArrayToInt( byte[] b)
    {
        int value = 0;

        if(b == null)
            return null;

        else
        {
            for (int i = 0; i < 4; i++)
                value = (value << 8) | (b[i] & 0xFF);
        }
        return value;
    }

    
    public static byte[] intToByteArray(int a)
    {
        byte[] ret = new byte[4];
        ret[3] = (byte) (a & 0xFF);
        ret[2] = (byte) ((a >> 8) & 0xFF);
        ret[1] = (byte) ((a >> 16) & 0xFF);
        ret[0] = (byte) ((a >> 24) & 0xFF);
        return ret;
    }

    // took it from here: http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
    
    public static byte[] concat( byte[] first,  byte[] second)
    {
      byte[] result = Arrays.copyOf(first, first.length + second.length);
      System.arraycopy(second, 0, result, first.length, second.length);
      return result;
    }

}
