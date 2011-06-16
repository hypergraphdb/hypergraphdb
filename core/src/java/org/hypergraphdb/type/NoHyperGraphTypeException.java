package org.hypergraphdb.type;

import java.net.URI;

import org.hypergraphdb.HGException;


/**
 * <p>
 * Thrown when the type system is not able to create a HyperGraph type for a given
 * type identifier (e.g. a class name). This happens when the type schema for 
 * the type identifier provided can't create a HGDB type for whatever reason. For
 * example, the Java type schema will fail to create a type for class without
 * a default constructor and that is not serializable.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class NoHyperGraphTypeException extends HGException
{
    private static final long serialVersionUID = -7823279582422706713L;

    public NoHyperGraphTypeException()
    {
        super("Unable to create HGDB type.");
    }

    public NoHyperGraphTypeException(String msg)
    {
        super(msg);
    }
    
    public NoHyperGraphTypeException(URI typeId)
    {
        super("Unable to create HGDB type for type identifier " + typeId);
    }
    
}