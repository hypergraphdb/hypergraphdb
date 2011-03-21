package org.hypergraphdb.util;

public class ChainResolver<Key, Value> implements RefResolver<Key, Value>
{
    private RefResolver<Key, Value> first, second;
    
    public ChainResolver(RefResolver<Key, Value> first, RefResolver<Key, Value> second)
    {
        this.first = first;
        this.second = second;
    }

    public Value resolve(Key key)
    {
        Value v = first.resolve(key);
        return v == null ? second.resolve(key) : v;
    }
}
