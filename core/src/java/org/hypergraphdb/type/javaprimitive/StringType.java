/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;

import org.hypergraphdb.HGException;


/**
 * <p>
 * The implementation of the primitive <code>String</code> type.
 * </p>
 *
 * <p>
 * This implementation should be used only for relatively small strings, things like
 * names or titles, since they are indexed and reference counted for lookup. 
 * If you want to store larger text data, including big documents, use the
 * <code>TextType</code> instead.  
 * </p>
 * 
 * <p>
 * A <code>java.lang.String</code> object is translated to a byte []  as 
 * follows:
 * 
 * <ul>
 * <li>The first 4 bytes consitute an unsigned integer - the reference count
 * for the string. The reference count is managed by the superclass</li>
 * <li>The fifth byte is a descriptor tag that indicates whether the string is
 * <code>null</code>, empty or it has actual content: 0 means null, 1 means empty
 * and 2 means we have something. In the future this might be extended to support
 * various encodings.</li>
 * <li>The rest of the bytes constitute the actual string with the default 8-bit
 * Java encoding.</li>
 * </ul>
 * 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class StringType extends PrimitiveTypeBase<String>
{
    public static final String INDEX_NAME = "hg_string_value_index";
    
    private Comparator<byte[]> comp = new StringComparator();
    
    public static class CaseInsensitiveStringComparator implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;
        public int compare(byte [] left, byte [] right)
        {            
            if (left[dataOffset] < 2)
                if (right[dataOffset] < 2)
                {
                    // null is considered < "" for the purpose of ordering
                    if (left[dataOffset] < right[dataOffset])
                        return -1;
                    else if (left[dataOffset] == right[dataOffset])
                        return 0;
                    else
                        return 1;
                }
                else
                    return -1;
            else if (right[dataOffset] < 2)
                return 1;
            else 
            {
                int i = dataOffset + 1;
                String lefts = new String(left, i, left.length - i);
                String rights = new String(right, i, right.length - i);
                //System.out.println(lefts + " -- " + rights);
                return lefts.compareToIgnoreCase(rights);
                /*
                InputStreamReader leftReader = new InputStreamReader(
                            new ByteArrayInputStream(left, i, left.length - i));
                InputStreamReader rightReader = new InputStreamReader(
                            new ByteArrayInputStream(right, i, right.length - i));
                try { while (true) {
                    int cl = leftReader.read();
                    int cr = rightReader.read();
                    if (cl == -1)
                        return (cr == -1) ? 0 : -1;
                    else if (cr == -1)
                        return 1;
                    int c = Character.toUpperCase(cl) - Character.toUpperCase(cr);
                    if (c != 0)
                        return c;
                    }
                }
                catch (IOException ex) { throw new HGException(ex); } */ 
            }
        }
    }
    
    public static class StringComparator implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;    	
        public int compare(byte [] left, byte [] right)
        {            
            if (left[dataOffset] < 2)
                if (right[dataOffset] < 2)
                {
                    // null is considered < "" for the purpose of ordering
                	if (left[dataOffset] < right[dataOffset])
                		return -1;
                	else if (left[dataOffset] == right[dataOffset])
                		return 0;
                	else
                		return 1;
                }
                else
                    return -1;
            else if (right[dataOffset] < 2)
                return 1;
            else 
            {
                int i = dataOffset + 1;
                for (; i < left.length && i < right.length; i++)
                    if (left[i] < right[i])
                        return -1;
                    else if (left[i] > right[i])
                        return 1;
               if (i == left.length)
                   if (i == right.length)
                       return 0;
                   else
                       return -1;
               else 
                   return 1;
            }
        }
    }
    
    public Comparator<byte[]> getComparator()
    {
        return comp;
    }
    
    protected String getIndexName()
    {
        return INDEX_NAME;
    }
    
    private byte [] stringToBytes(String s)
    {
        byte [] data;
        
        if (s == null)
        {
            data = new byte[1];
            data[0] = 0;
        }
        else if (s.length() == 0)
        {
            data = new byte[1];
            data[0] = 1;
        }
        else
        {
            byte [] asBytes = s.getBytes();
            data = new byte[1 + asBytes.length];
            data[0] = 2;
            System.arraycopy(asBytes, 0, data, 1, asBytes.length);
        }
        return data;
    }
    
    protected byte [] writeBytes(String value)
    {
        return stringToBytes(value); 
    }
    
    protected String readBytes(byte [] data, int offset)
    {
        switch (data[offset])
        {
            case 0: return null;
            case 1: return "";
            default: return new String(data, offset + 1, data.length - offset - 1);
        }
    }
    
    public boolean isCaseSensitive()
    {
        return comp instanceof StringComparator;
    }
    
    public void setCaseSensitive(boolean caseSensitive)
    {
        if (caseSensitive)
            comp = new StringComparator();
        else
            comp = new CaseInsensitiveStringComparator();
    }
}
