/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.cond2qry;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;

public interface ConditionToQuery<T>
{
	HGQuery<T> getQuery(HyperGraph graph, HGQueryCondition condition);
	QueryMetaData getMetaData(HyperGraph graph, HGQueryCondition condition);
}
