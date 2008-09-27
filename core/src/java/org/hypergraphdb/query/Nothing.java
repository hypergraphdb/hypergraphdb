package org.hypergraphdb.query;

/**
 * <p>
 * This condition represents the negation of everything. It will yield an empty result set
 * alone or in conjunction with any other condition. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public final class Nothing implements HGQueryCondition 
{
	public static final Nothing Instance = new Nothing();
	
	/**
	 * this is required to ensure the class is a bean.
	 */
	public Nothing() { }	
}