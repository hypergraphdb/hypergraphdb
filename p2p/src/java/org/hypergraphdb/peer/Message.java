/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.Collection;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mjson.Json;


public class Message // extends Json
{
    //private Map<String, Object> map;
	Json json;
	
    public Message()
    {
        json = Json.object();
    }

    public Message(Map<String, Object> map)
    {
        json = Json.make(map);
    }

    @Override
    public Map<String, Json> asJsonMap() { return json.asJsonMap(); }
    
//    public Performative getPerformative()
//    {
//        return Performative.toConstant(json.at(Messages.PERFORMATIVE).asString());
//    }
    
//    public UUID getConversationId()
//    {
//        return (UUID)get(Messages.CONVERSATION_ID);
//    }
    
//    @SuppressWarnings("unchecked")
//    public <T> T getContent()
//    {
//        return (T)get(Messages.CONTENT);
//    }
    
//    public void clear()
//    {
//        map.clear();
//    }

//    public boolean containsKey(Object key)
//    {
//        return map.containsKey(key);
//    }
//
//    public boolean containsValue(Object value)
//    {
//        return map.containsValue(value);
//    }
//
//    public Set<java.util.Map.Entry<String, Object>> entrySet()
//    {
//        return map.entrySet();
//    }

//    public Object get(String key)
//    {
//        return json.at(key);
//    }

//    public boolean isEmpty()
//    {
//        return map.isEmpty();
//    }

//    public Set<String> keySet()
//    {
//        return map.keySet();
//    }

//    public Object put(String key, Object value)
//    {
//        return json.set(key, value);
//    }

//    public void putAll(Map<? extends String, ? extends Object> m)
//    {
//        map.putAll(m);
//    }

//    public Object remove(Object key)
//    {
//        return map.remove(key);
//    }

//    public int size()
//    {
//        return json.asJsonMap().size();
//    }

//    public Collection<Object> values()
//    {
//        return map.values();
//    }
    
    public String toString()
    {
        return json.toString();
    }
}
