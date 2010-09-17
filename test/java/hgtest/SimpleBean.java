package hgtest;

/**
 * A simple bean with a property for each of the primitive Java types.
 * 
 * @author bolerio
 */
public class SimpleBean 
{
	private int intProp = 32000;
	private char charProp = 'a';
	private boolean boolProp = true;
	private short shortProp = 15000;
	private long longProp = 100000L;
	private byte byteProp = (byte)-250;
	private float floatProp = 4.5f;
	private double doubleProp = 100.234;
	private String strProp = "This is a string";
	private String strPropEmpty = "";
	private String strPropNull = null;
	
	public final boolean isBoolProp() {
		return boolProp;
	}
	public final void setBoolProp(boolean boolProp) {
		this.boolProp = boolProp;
	}
	public final byte getByteProp() {
		return byteProp;
	}
	public final void setByteProp(byte byteProp) {
		this.byteProp = byteProp;
	}
	public final char getCharProp() {
		return charProp;
	}
	public final void setCharProp(char charProp) {
		this.charProp = charProp;
	}
	public final double getDoubleProp() {
		return doubleProp;
	}
	public final void setDoubleProp(double doubleProp) {
		this.doubleProp = doubleProp;
	}
	public final float getFloatProp() {
		return floatProp;
	}
	public final void setFloatProp(float floatProp) {
		this.floatProp = floatProp;
	}
	public final int getIntProp() {
		return intProp;
	}
	public final void setIntProp(int intProp) {
		this.intProp = intProp;
	}
	public final long getLongProp() {
		return longProp;
	}
	public final void setLongProp(long longProp) {
		this.longProp = longProp;
	}
	public final short getShortProp() {
		return shortProp;
	}
	public final void setShortProp(short shortProp) {
		this.shortProp = shortProp;
	}
	public final String getStrProp() {
		return strProp;
	}
	public final void setStrProp(String strProp) {
		this.strProp = strProp;
	}
	public final String getStrPropEmpty() {
		return strPropEmpty;
	}
	public final void setStrPropEmpty(String strPropEmpty) {
		this.strPropEmpty = strPropEmpty;
	}
	public final String getStrPropNull() {
		return strPropNull;
	}
	public final void setStrPropNull(String strPropNull) {
		this.strPropNull = strPropNull;
	}	
}