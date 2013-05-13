package org.hypergraphdb.util;

import java.util.Map;

public class MapResolver<Key, Value> implements RefResolver<Key, Value>
{
    private Map<Key, Value> map;
    
    public MapResolver(Map<Key, Value> map)
    {
        this.map = map;
    }
    
    public Value resolve(Key key)
    {
        return map.get(key);
    }
    
    public Map<Key, Value> getMap() { return map; }
}