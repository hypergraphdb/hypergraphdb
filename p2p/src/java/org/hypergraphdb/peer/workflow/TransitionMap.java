/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.workflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mjson.Json;

/**
 * <p>
 * Holds the definition of the FSM (Finite State Machine) associated with a
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
    private Map<String, Set<Transition>> map =  
        Collections.synchronizedMap(new HashMap<String, Set<Transition>>()); 
    private Map<Transition, Set<String>> inverseMap =
        Collections.synchronizedMap(new HashMap<Transition, Set<String>>());
    
    private Map<String, Transition> activityMap = 
        Collections.synchronizedMap(new HashMap<String, Transition>());
    
    /**
     * <p>
     * Try to find an appropriate transition given the set of message attributes.
     * Transition defines for each of the attributes are considered and only if
     * a unique matching transition can be determined, it will be returned. Otherwise,
     * an exception is thrown. Unique matching is determined as follows:
     * <ol>
     * <li>For each message attribute <code>A</code>, consider the set of transitions
     * <code>S(A)</code> defined for it. Ignore attributes for which no transitions have
     * been defined.</li>
     * <li>Consider the intersection <code>I</code> of all those sets <code>S(A)</code>.</li> 
     * <li>If all sets <code>S(A)</code> interest into a single transition <code>T</code>, 
     * i.e. if <code>I</code> has only one element, then that transition is returned.</li>
     * <li>If <code>I</code> has no element that means that attributes point to different 
     * transitions and there's ambiguity - we throw an exception.</li>
     * <li>If <code>I</code> has more than one transition in it, then we try to find the 
     * unique one (if any) that is pointed exactly by the set of all <code>A</code>s and by nothing 
     * else. 
     * <li> 
     * </ol> 
     * The reasoning behind the last rule might not be obvious: it allows one to define 
     * general transitions that are overriden for more specific cases. For example one could
     * have a transition that's based on pair of attributes (performative=X, ontology=Y) and
     * another that's based on a similar pair (performative=X, ontology=Z) that differs in the
     * <code>ontology</code> attribute. One could then define a third transition that's based solely on the
     * <code>performative=X</code> attribute and that handles all other cases where 
     * <code>ontology != Y</code> and <code>ontology != Z</code>.
     * </p>
     * 
     * @param fromState
     * @param messageAttributes
     * @return
     */
    public Transition getTransition(WorkflowStateConstant fromState, 
                                    Json messageAttributes)
    {
        Set<Transition> candidates = null;
        Set<String> foundKeys = null;
        for (Map.Entry<String, Json> e : messageAttributes.asJsonMap().entrySet())
        {
        	if (e.getValue().isNull())
        		continue;
            String key = fromState.toString() + "&" + e.getKey() + "=" + e.getValue().getValue();
            Set<Transition> S = map.get(key);
            if (S == null)
                continue;
            if (candidates == null)
            {
                candidates = new HashSet<Transition>();
                candidates.addAll(S);
                foundKeys = new HashSet<String>();
            }
            else for (Transition t : candidates)
                if (!S.contains(t))
                    candidates.remove(t);
            foundKeys.add(key);            
        }
        if (candidates == null) // nothing found
            return null;
        else switch (candidates.size())
        {
            case 1: return candidates.iterator().next();
            case 0: throw new RuntimeException("Ambiguous transition for message attributes " + 
                                                messageAttributes);
            default: 
                // We try to find a transition in the set that's uniquely determined
                // by the attributes: is exactly one of the candidate transitions has
                // as its inverse set the set of attributes that determined the candidates
                // that's the transition we want...otherwise we have ambiguity again.
            {
                Transition result = null;
                for (Transition can : candidates)
                    if (inverseMap.get(can).equals(foundKeys))
                        if (result == null)
                            result = can;
                        else throw new RuntimeException("Ambiguous transition for message attributes " + 
                                                        messageAttributes);
                if (result == null)
                    throw new RuntimeException("Ambiguous transition for message attributes " + 
                                               messageAttributes);
                else
                    return result;
            }
        }
    }

    public void setTransition(WorkflowStateConstant fromState, 
                              Map<String, String> messageAttributes,
                              Transition transition)
    {
        if (messageAttributes == null || messageAttributes.isEmpty())
            throw new IllegalArgumentException(
                "At least some message attributes should be specified for a state transition.");
        Set<String> inverseSet = new HashSet<String>();
        for (Map.Entry<String, String> e : messageAttributes.entrySet())
        {
            String key = fromState.toString() + "&" + e.getKey() + "=" + e.getValue();
            Set<Transition> S = map.get(key);
            if (S == null)
            {
                S = new HashSet<Transition>();
                map.put(key, S);
            }
            S.add(transition);
            inverseSet.add(key);
        }
        inverseMap.put(transition, inverseSet);
    }
    
    public Transition getTransition(WorkflowStateConstant fromState,
                                    Activity fromActivity,
                                    WorkflowStateConstant atActivityState)
    {
        String key = fromState.toString() + "&" + fromActivity.getType() + "&" +
                     atActivityState.toString();
        return activityMap.get(key);
    }
    
    public void setTransition(WorkflowStateConstant fromState,
                              String subActivityType,
                              WorkflowStateConstant atSubActivityState,
                              Transition t)
    {
        synchronized (activityMap)
        {
            String key = fromState.toString() + "&" + subActivityType + "&" +
                         atSubActivityState.toString();
            Transition existing = activityMap.get(key);
            if (existing != null)
                throw new RuntimeException("A transition alread exist from state " + fromState + 
                                           " and sub-activity " + subActivityType + " at state " +
                                           atSubActivityState);
            else
                activityMap.put(key, t);
        }
    } 
}
