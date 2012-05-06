package org.hypergraphdb.handle;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAUtils;

public class IntPersistentHandle implements HGPersistentHandle
{
  private static final long serialVersionUID = -9186528557514443555L;

  int x;
  byte [] buffer = new byte[4];
  
  public IntPersistentHandle(int value)
  {
      x = value;
      BAUtils.writeInt(x, buffer, 0);        
  }
  
  public byte[] toByteArray()
  {
      return buffer;
  }

  public int compareTo(HGPersistentHandle o)
  {
      return x - ((IntPersistentHandle)o).x; 
  }

  public int hashCode()
  {
      final int prime = 31;
      int result = 1;
      result = prime * result + x;
      return result;
  }

  public boolean equals(Object obj)
  {
      if (this == obj)
          return true;
      if (obj == null)
          return false;
      if (obj instanceof HGLiveHandle)
          obj = ((HGLiveHandle)obj).getPersistent();        
      if (getClass() != obj.getClass())
          return false;
      IntPersistentHandle other = (IntPersistentHandle) obj;
      if (x != other.x)
          return false;
      return true;
  }

  public String toString()
  {
      return "intHandle(" + Integer.toString(x) + ")";
  }

  public String toStringValue()
  {
      return Integer.toString(x);
  }

  public HGPersistentHandle getPersistent()
  {
      return this;
  }    
}