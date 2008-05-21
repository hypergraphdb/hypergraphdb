/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;


/**
 * <p>
 * A <code>HGRandomAccessResult</code> is a search result that is based on some
 * kind of cursor that allows immediate positionning on some result value, if it
 * exists. This is particularly useful for index-based search results where the whole
 * result set is ordered and provides quick lookup to being with. A query execution
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
		 * Indicates that the element was found and the cursor is positionned on that
		 * element.
		 */
		found,
		
		/**
		 * Indicates that the element was not found and the cursor has <strong>NOT</strong>
		 * changed position.
		 */
		nothing,
		
		/**
		 * Indicates that the element was not found, but the cursor was positionned to 
		 * the next element greater than the element sought for.
		 */
		close
	}
	
	/**
	 * <p>Position the result set at a particular value if that value
	 * is indeed part of the result set.
	 * </p>
	 * 
	 * @param value The value where this result set should be positionned.
	 * @param exactMatch A flag indicating whether the passed in value should
	 * match exactly a value in the result set, or whether the cursor should
	 * be positionned to the closest value. Here "closest" means "smallest 
	 * greater than the <code>value</code> parameter.
	 * @return A <code>GotoResult</code>.
	 */
	GotoResult goTo(ValueType value, boolean exactMatch);
}