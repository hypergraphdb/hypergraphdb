/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

/**
 * <p>
 * Store meta information about a particular activity type.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class ActivityType
{
    private String name;
    private TransitionMap transitionMap = new TransitionMap();
    private ActivityFactory factory;
    
    public ActivityType(String name, ActivityFactory factory)
    {
        this.name = name;
        this.factory = factory;
    }
    
    public String getName()
    {
        return name;
    }
    
    public TransitionMap getTransitionMap()
    {
        return this.transitionMap;
    }
    
    public ActivityFactory getFactory()
    {
        return factory;
    }
}
