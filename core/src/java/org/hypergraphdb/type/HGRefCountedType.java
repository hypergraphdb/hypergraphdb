/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type;

/**
 * <p>
 * A marker interface indicating that a given {@link HGAtomType} implementation
 * is reference-counted. This means that distinct values of the given type
 * are stored only once in permanent storage and a count is maintained for
 * each separate enclosing type or atom that references them.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface HGRefCountedType
{
}
