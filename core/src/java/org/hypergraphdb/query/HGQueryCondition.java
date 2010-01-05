/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

/**
 * <p>
 * The interface defines a HyperGraph query condition. A condition
 * is either a primitive condition or a logical operator over other
 * conditions. 
 * </p>
 * 
 * <p>
 * The <code>HGQueryCondition</code>s does not mandate at what level, 
 * HyperGraph atoms or HGStore, will conditions apply. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGQueryCondition
{
}
