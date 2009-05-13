package org.hypergraphdb.peer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class Message implements Map<String, Object>
{
    private Map<String, Object> map;

    public Message()
    {
        map = new HashMap<String, Object>();
    }

    public Message(Map<String, Object> map)
    {
        this.map = map;
    }

    public Performative getPerformative()
    {
        return Performative.toConstant((String)get(Messages.PERFORMATIVE));
    }
    
    public UUID getConversationId()
    {
        return (UUID)get(Messages.CONVERSATION_ID);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getContent()
    {
        return (T)get(Messages.CONTENT);
    }
    
    public void clear()
    {
        map.clear();
    }

    public boolean containsKey(Object key)
    {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        return map.containsValue(value);
    }

    public Set<java.util.Map.Entry<String, Object>> entrySet()
    {
        return map.entrySet();
    }

    public Object get(Object key)
    {
        return map.get(key);
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public Set<String> keySet()
    {
        return map.keySet();
    }

    public Object put(String key, Object value)
    {
        return map.put(key, value);
    }

    public void putAll(Map<? extends String, ? extends Object> m)
    {
        map.putAll(m);
    }

    public Object remove(Object key)
    {
        return map.remove(key);
    }

    public int size()
    {
        return map.size();
    }

    public Collection<Object> values()
    {
        return map.values();
    }
    
    public String toString()
    {
        return map.toString();
    }
}
