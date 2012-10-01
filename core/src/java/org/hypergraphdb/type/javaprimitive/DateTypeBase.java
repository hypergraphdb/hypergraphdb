package org.hypergraphdb.type.javaprimitive;


import java.util.Comparator;
import java.util.TimeZone;

public abstract class DateTypeBase<JavaType> extends PrimitiveTypeBase<JavaType>
{
    public static final String INDEX_NAME = "hg_time_value_index";

    protected abstract long toLong(JavaType x);
    protected abstract JavaType fromLong(long x);
    
    protected String getIndexName()
    {
        return INDEX_NAME;
    }

    protected JavaType readBytes(byte[] bytes, int offset)
    {
        return fromLong(bytesToLong(bytes,offset));
    }
    
    private static long bytesToLong(byte[] bytes, int offset)
    {
        long x = (((long)bytes[offset] << 56) +
                ((long)(bytes[offset + 1] & 255) << 48) +
                ((long)(bytes[offset + 2] & 255) << 40) +
                ((long)(bytes[offset + 3] & 255) << 32) +
                ((long)(bytes[offset + 4] & 255) << 24) +
                ((bytes[offset + 5] & 255) << 16) + 
                ((bytes[offset + 6] & 255) <<  8) + 
                ((bytes[offset + 7] & 255) <<  0));         
        return x + TimeZone.getDefault().getRawOffset();
    }

    protected byte[] writeBytes(JavaType value)
    {
        byte [] data = new byte[8];
        long v = toLong(value) - TimeZone.getDefault().getRawOffset();
        data[0] = (byte) ((v >>> 56)); 
        data[1] = (byte) ((v >>> 48));
        data[2] = (byte) ((v >>> 40)); 
        data[3] = (byte) ((v >>> 32));
        data[4] = (byte) ((v >>> 24)); 
        data[5] = (byte) ((v >>> 16));
        data[6] = (byte) ((v >>> 8)); 
        data[7] = (byte) ((v >>> 0));
        return data;
   }

    private Comparator<byte[]> comparator = null;
    
    public static class DateComparator<T> implements Comparator<byte[]>, java.io.Serializable 
    {
        private static final long serialVersionUID = 1L;
        
        public int compare(byte [] left, byte [] right)
        {
            long l = bytesToLong(left, dataOffset);
            long r = bytesToLong(right, dataOffset);
            return Long.signum(l-r);
        }
    };
    
    public Comparator<byte[]> getComparator()
    {
        if (comparator == null)
            comparator = new DateComparator<JavaType>();
        return comparator;
    }
}