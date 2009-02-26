package org.hypergraphdb.peer.workflow;

/**
 * <p>
 * A simple bean to hold the outcome of an activity once it has finished execution.
 * The activity might have completed successfully, or it might have failed or it
 * might have been explicitly canceled at some point. This will be indicated by
 * its state. In case it failed, there might by some additional information on why
 * it failed - either an exception that occurred at this peer or due to a remote
 * exception 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public final class ActivityResult
{
    Activity activity;
    Throwable exception;
    
    public ActivityResult(Activity activity)
    {
        this.activity = activity;
    }
    
    public ActivityResult(Activity activity, Throwable exception)
    {
        this(activity);
        this.exception = exception;
    }
    
    public Activity getActivity()
    {
        return activity;
    }
    public Throwable getException()
    {
        return exception;
    }
}