/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

public class WorkflowStateConstant extends WorkflowState
{
    WorkflowStateConstant(String name)
    {
        super(name);
        listeners = null;
    }

    @Override
    public void addListener(StateListener l)
    {
        throw new RuntimeException("Can't add a state change listener to a constant state.");
    }

    @Override
    public void assign(WorkflowStateConstant arg0)
    {
        throw new RuntimeException("Cannot modify a WorkflowState constant.");
    }

    @Override
    public boolean compareAndAssign(WorkflowStateConstant arg0,
                                    WorkflowStateConstant arg1)
    {
        throw new RuntimeException("Cannot make a state change on a WorkflowState constant.");
    }

    @Override
    public void removeListener(StateListener l)
    {
        throw new RuntimeException("Can't remove a state change listener from a constant state.");
    }   
}
