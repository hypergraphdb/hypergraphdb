package hgtest.beans;

/**
 * A simple bean with a property for each array of a primitive.
 * 
 * @author bolerio
 */
public class SimpleBeanArray
{
    private int[] intPropArray = new int[] { 32000, 32001, 32003 };
    private char[] charPropArray = new char[] { 'a', 'b', 'c' };
    private boolean[] boolPropArray = new boolean[] { true, false };
    private short[] shortPropArray = new short[] {}; // test 0-length array
    private long[] longPropArray = null; // test nulls
    private byte[] bytePropArray = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    private float[] floatPropArray = new float[] { 4.5f };
    private double[] doublePropArray = new double[] { 100.234, 5, 65, 3245,
            234, 234.655 };
    private String[] strPropArray = new String[] { "This is a string", "",
            null, "This is another and a final string" };
    private SimpleBean[] sbArray = new SimpleBean[] { new SimpleBean(),
            new SimpleBean() };

    public final boolean[] getBoolPropArray()
    {
        return boolPropArray;
    }

    public final void setBoolPropArray(boolean[] boolPropArray)
    {
        this.boolPropArray = boolPropArray;
    }

    public final byte[] getBytePropArray()
    {
        return bytePropArray;
    }

    public final void setBytePropArray(byte[] bytePropArray)
    {
        this.bytePropArray = bytePropArray;
    }

    public final char[] getCharPropArray()
    {
        return charPropArray;
    }

    public final void setCharPropArray(char[] charPropArray)
    {
        this.charPropArray = charPropArray;
    }

    public final double[] getDoublePropArray()
    {
        return doublePropArray;
    }

    public final void setDoublePropArray(double[] doublePropArray)
    {
        this.doublePropArray = doublePropArray;
    }

    public final float[] getFloatPropArray()
    {
        return floatPropArray;
    }

    public final void setFloatPropArray(float[] floatPropArray)
    {
        this.floatPropArray = floatPropArray;
    }

    public final int[] getIntPropArray()
    {
        return intPropArray;
    }

    public final void setIntPropArray(int[] intPropArray)
    {
        this.intPropArray = intPropArray;
    }

    public final long[] getLongPropArray()
    {
        return longPropArray;
    }

    public final void setLongPropArray(long[] longPropArray)
    {
        this.longPropArray = longPropArray;
    }

    public final short[] getShortPropArray()
    {
        return shortPropArray;
    }

    public final void setShortPropArray(short[] shortPropArray)
    {
        this.shortPropArray = shortPropArray;
    }

    public final String[] getStrPropArray()
    {
        return strPropArray;
    }

    public final void setStrPropArray(String[] strPropArray)
    {
        this.strPropArray = strPropArray;
    }

    public SimpleBean[] getSbArray()
    {
        return sbArray;
    }

    public void setSbArray(SimpleBean[] sbArray)
    {
        this.sbArray = sbArray;
    }
}