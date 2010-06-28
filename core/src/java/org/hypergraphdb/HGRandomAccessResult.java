/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;


/**
 * <p>
 * A <code>HGRandomAccessResult</code> is a search result that is based on some
 * kind of cursor that allows immediate positioning on some result value, if it
 * exists. This is particularly useful for index-based search results where the whole
 * result set is ordered and provides quick lookup to begin with. A query execution
 * plan may take advantage of this capability.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGRandomAccessResult<ValueType> extends HGSearchResult<ValueType> 
{
	/**
	 * <p>
	 * Enumerates the possible results of a call to the <code>goTo</code> in a 
	 * <code>HGRandomAccessResult</code>.
	 * </p>
	 * 
	 * @author Borislav Iordanov
	 */
	enum GotoResult 
	{ 
		/**
		 * Indicates that the element was found and the cursor is positioned on that
		 * element.
		 */
		found,
		
		/**
		 * Indicates that the element was not found and the cursor has <strong>NOT</strong>
		 * changed position.
		 */
		nothing,
		
		/**
		 * Indicates that the element was not found, but the cursor was positioned to 
		 * the next element greater than the element sought for.
		 */
		close
	}
	
	/**
	 * <p>Position the result set at a particular value if that value
	 * is indeed part of the result set.
	 * </p>
	 * 
	 * @param value The value where this result set should be positioned.
	 * @param exactMatch A flag indicating whether the passed in value should
	 * match exactly a value in the result set, or whether the cursor should
	 * be positioned to the closest value. Here "closest" means "smallest 
	 * greater than the <code>value</code> parameter.
	 * @return A <code>GotoResult</code>.
	 */
	GotoResult goTo(ValueType value, boolean exactMatch);
	
	/**
	 * <p>Move the cursor of this result set after the last result. When positioned 
	 * after the last result, there is no current element, <code>hasNext</code> will
	 * return false and <code>hasPrev</code> will return true if there's at least
	 * one element in this result set.</p>
	 */
	void goAfterLast();

    /**
     * <p>Move the cursor of this result set before the first result. This is equivalent
     * to resetting the cursor to its initial state, right after the result set was
     * created. When positioned 
     * before the first result, there is no current element, <code>hasPrev</code> will
     * return false and <code>hasNext</code> will return true if there's at least
     * one element in this result set.</p>
     */	
	void goBeforeFirst();
}