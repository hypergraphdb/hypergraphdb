package org.hypergraphdb.peer.workflow;

import java.util.Map;

/**
 * <p>
 * Holds the definition of the FSM (Finiate State Machine) associated with an
 * particular <code>Activity</code> type. There are two kinds of transitions
 * that can be associated with an activity:
 * 
 * <ul>
 * <li>
 * <b>message transitions</b> are triggered upon reception of a new message
 * by another peer.
 * </li>
 * <li>
 * <b>sub-activity transitions</b> are triggered upon a state of an activity 
 * of which the activity of interest is a parent. 
 * </li>
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class TransitionMap
{

    public Transition getTransition(WorkflowStateConstant fromState, 
                                    Map<String, Object> message)
    {
        return null;
    }

    public void setTransition(WorkflowStateConstant fromState, 
                              Map<String, String> messageAttributes,
                              Transition transition)
    {
    }
    
    public Transition getTransition(WorkflowStateConstant fromState,
                                    Activity fromActivity,
                                    WorkflowStateConstant atActivityState)
    {
        return null;
    }
    
    public void setTransition(WorkflowStateConstant fromState,
                              String subActivityType,
                              WorkflowStateConstant atSubActivityState)
    {
    } 
}