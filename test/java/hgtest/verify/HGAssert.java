package hgtest.verify;

import org.testng.Assert;

public class HGAssert
{
    public static void assertEquals(int [] A, int [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(boolean [] A, boolean [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(byte [] A, byte [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(char [] A, char [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B.toString() + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A.toString());
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(double [] A, double [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(float [] A, float [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(short [] A, short [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEquals(long [] A, long [] B)
    {
        if (A == B)
            return;
        else if (A == null)
            throw new AssertionError("Expected " + B + ", but got null.");
        else if (B == null)
            throw new AssertionError("Expected null, but got " + A);
        else if (A.length != B.length)
            throw new AssertionError("Expected array of length " + B.length + ", but got length " + A.length);
        else for (int i = 0; i < A.length; i++)
            Assert.assertEquals(A[i], B[i]);        
    }
    
    public static void assertEqualsDispatch(Object x, Object y)
    {
        if (x == y)
            return;
        
        if (x instanceof boolean [])
            assertEquals((boolean[])x, (boolean[])y);
        else if (x instanceof byte [])
            assertEquals((byte[])x, (byte[])y);
        else if (x instanceof char [])
            assertEquals((char[])x, (char[])y);
        else if (x instanceof double [])
            assertEquals((double[])x, (double[])y);
        else if (x instanceof float [])
            assertEquals((float[])x, (float[])y);
        else if (x instanceof int [])
            assertEquals((int[])x, (int[])y);
        else if (x instanceof long [])
            assertEquals((long[])x, (long[])y);
        else if (x instanceof short[])
            assertEquals((short[])x, (short[])y);
        else if (x instanceof Object[])
            Assert.assertEquals((Object[])x, (Object[])y);
        else 
            Assert.assertEquals(x, y);        
    }
}