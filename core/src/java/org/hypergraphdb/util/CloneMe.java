package org.hypergraphdb.util;

/**
 * <p>
 * Expose the <code>clone</code> method publicly so that it can be invoked
 * at an interface/abstract class level.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface CloneMe extends Cloneable
{
    <T> T duplicate();    
}