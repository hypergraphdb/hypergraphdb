package org.hypergraphdb.util;

import java.util.Map;

/**
 * <p>
 * A map-based resolver that will delegate to another resolved if the element is not
 * in the map. This is used to override a wrapped {@link RefResolver}, for example for
 * scoping of some sort.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <Key>
 * @param <Value>
 */
public class DelegateMapResolver<Key, Value> extends MapResolver<Key, Value>
{
    RefResolver<Key, Value> delegate;
    
    public DelegateMapResolver(RefResolver<Key, Value> delegate, Map<Key, Value> map)
    {        
        super(map);
        this.delegate = delegate;
    }
    
    @Override
    public Value resolve(Key key)
    {
        Value v = super.resolve(key);
        return (v == null) ? delegate.resolve(key) : v;            
    }
}