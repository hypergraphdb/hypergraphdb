package org.hypergraphdb.util;

public class KeyMapResolver<Key, Value> implements RefResolver<Key, Value>
{
    private RefResolver<Key, Key> keyMap;
    private RefResolver<Key, Value> refResolver;
    
    public KeyMapResolver(RefResolver<Key, Key> keyMap, RefResolver<Key, Value> refResolver)
    {
        this.keyMap = keyMap;
        this.refResolver = refResolver;
    }
    
    public Value resolve(Key key)
    {
        Key realKey = keyMap.resolve(key);
        if (realKey == null)
            realKey = key;
        return refResolver.resolve(realKey);
    }

}
