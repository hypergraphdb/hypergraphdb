/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;


import java.lang.reflect.Constructor;
import java.util.UUID;

import mjson.Json;

import org.hypergraphdb.peer.HyperGraphPeer;

/**
 * <p>
 * A <code>DefaultActivityFactory</code> creates new activities through 
 * reflection on their <code>activityClass</code> member variable. The factory
 * will attempt to construct an instance with the full set of the parameters
 * of the <code>make</code> given that such a constructor exists. Otherwise, it
 * will attempt with a reduced set, and possibly an empty set of parameters where
 * it just expects a default constructor.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class DefaultActivityFactory implements ActivityFactory
{
    private Class<? extends Activity> activityClass;
    private Constructor<? extends Activity> constructor = null;
    
    public DefaultActivityFactory(Class<? extends Activity> activityClass)
    {
        this.activityClass = activityClass;
        try
        {
            constructor = activityClass.getConstructor(HyperGraphPeer.class, UUID.class, Json.class);
        }         
        catch (NoSuchMethodException e) { }
        
        if (constructor == null) try 
        {
            constructor = activityClass.getConstructor(HyperGraphPeer.class, UUID.class);
        }
        catch (NoSuchMethodException ex) { }
        
        if (constructor == null) try 
        {
            constructor = activityClass.getConstructor(HyperGraphPeer.class);
        }
        catch (NoSuchMethodException ex) { }
        
        if (constructor == null) try
        {
            constructor = activityClass.getConstructor();
        }
        catch (NoSuchMethodException ex) 
        { 
            throw new RuntimeException("Can't find appropriate constructor for class " + 
                                       activityClass.getName());
        }
    }
    
    public Activity make(HyperGraphPeer thisPeer, UUID id, Json msg)
    {
        try
        {            
            if (constructor.getParameterTypes().length == 0)
                return constructor.newInstance();
            else if (constructor.getParameterTypes().length == 1)
                return constructor.newInstance(thisPeer);
            else if (constructor.getParameterTypes().length == 2)                
                return constructor.newInstance(thisPeer, id);
            else
                return constructor.newInstance(thisPeer, id, msg);
        } 
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Class<?> getActivityClass()
    {
        return activityClass;
    }
}
