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