package org.hypergraphdb.peer.serializer;

import java.beans.IndexedPropertyDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.PhantomHandle;
import org.hypergraphdb.handle.PhantomManagedHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.handle.WeakHandle;
import org.hypergraphdb.handle.WeakManagedHandle;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.type.BonesOfBeans;
import org.hypergraphdb.util.Constant;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.TwoWayMap;

public class HGPeerJsonFactory implements Json.Factory
{
	HyperGraph graph;
	Json.Factory f = Json.defaultFactory;
	TwoWayMap<String, String> shortNameMap = new TwoWayMap<String, String>();
	Map<String, JsonConverter> converterMap = new ConcurrentHashMap<String, JsonConverter>();
	
	static HGPeerJsonFactory instance = new HGPeerJsonFactory();
	static
	{
		instance.converterMap.put(UUID.class.getName(), instance.uuidConverter);
		instance.converterMap.put(PhantomManagedHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(PhantomHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(WeakHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(WeakManagedHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(UUIDPersistentHandle.class.getName(), instance.handleConverter);		
		instance.converterMap.put(Constant.class.getName(), instance.constantConverter);
		instance.converterMap.put(AtomValueCondition.class.getName(), 
				instance.new BeanJsonConverter(new String[] {"operator", "valueReference"}));
		instance.converterMap.put(AtomTypeCondition.class.getName(), 
				instance.new BeanJsonConverter(new String[] {"typeReference"}));
	}
	
	public HGPeerJsonFactory setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
		return this;
	}
	public static HGPeerJsonFactory getInstance() { return instance; }
	
	public static interface JsonConverter 
	{
		Json to(Object x);
		Object from(Json x);
	}
	
	public final JsonConverter constantConverter = new JsonConverter()
	{
		public Json to(Object x) 
		{			
			return Json.object("javaType", ((Constant)x).get().getClass().getName(),
						       "value", make(((Constant)x).get())); 
		}
		public Object from(Json x) 
		{
			try {
				Class cl = Class.forName(x.at("javaType").asString());
				if (Number.class.isAssignableFrom(cl))
				{
					// we're instantiating from a string to avoid an if-then-else for each
					// every Java primitive number type
					return new Constant(
							cl.getConstructor(String.class).newInstance(
									x.at("value").getValue().toString()));
				}
				else
					return new Constant(value(x.at("value")));	
			} catch (Exception ex)
			{
				throw new RuntimeException(ex);
			} 
		}		
	};

	public final JsonConverter uuidConverter = new JsonConverter()
	{
		public Json to(Object x) { return Json.make(((UUID)x).toString()); }
		public Object from(Json x) { return UUID.fromString(x.asString()); }		
	};

	public final JsonConverter handleConverter = new JsonConverter()
	{
		public Json to(Object x) { return Json.make(((HGHandle)x).getPersistent().toString()); }
		public Object from(Json x) { return graph.getHandleFactory().makeHandle(x.asString()); }		
	};
	
	// TODO: doesn't deal with recursive structures. Not clear if an attempt should be
	// made to represent them or simply throw an exception.
	//
	// The format for a bean is { "javaType": "either full class name or short name", 
	//   all properties of the bean ....} 
	// So the presence of a "javaType" property is what indicates that we have a bean.
	public final class BeanJsonConverter implements JsonConverter 
	{
		String[] props;
		public BeanJsonConverter() { }
		public BeanJsonConverter(String [] props) { this.props = props; }
		
		public Json to(Object x)
		{
			String typeName = shortNameMap.containsX(x.getClass().getName()) ?
							  shortNameMap.getY(x.getClass().getName()) : x.getClass().getName();
            Json result = Json.object("javaType", typeName);            
            try
            {
                if (props != null)
                    for (String propname : props)
                    {
                        PropertyDescriptor desc = BonesOfBeans.getPropertyDescriptor(x,
                                                                                     propname);
                        result.set(propname, Json.make(BonesOfBeans.getProperty(x, desc)));
                    }
                else
                    for (PropertyDescriptor desc : BonesOfBeans.getAllPropertyDescriptors(x)
                            .values())
                        if (desc.getReadMethod() != null
                                && desc.getWriteMethod() != null)
                        {
                            result.set(desc.getName(), Json.make(BonesOfBeans.getProperty(x, desc)));
                        }
                return result;
            }
            catch (Throwable ex)
            {
                HGUtils.throwRuntimeException(ex);
            }
            return null; // unreachable
			
		}
		
		public Object from(Json x)
		{
			if (!x.isObject())
				return x.getValue();
			Json typeName = x.at("javaType");
			if (typeName == null)
			{
				// Not a bean, must be simply a map
        		HashMap<String, Object> m = new HashMap<String, Object>();
        		for (Map.Entry<String, Json> e : x.asJsonMap().entrySet())
        			m.put(e.getKey(), from(e.getValue()));
        		return m;
			}
			String fullName = shortNameMap.getX(typeName.asString());
			if (fullName == null) fullName = typeName.asString();
			try
			{
				Object bean = Class.forName(fullName).newInstance();
		        for (Map.Entry<String, Json> entry : x.asJsonMap().entrySet())
		        {
		            PropertyDescriptor descriptor = BonesOfBeans.getPropertyDescriptor(bean,
		                                                                               entry.getKey());
		            if (descriptor == null)
		            	continue;
		            Class<?> propertyClass = descriptor.getPropertyType();
		            Object value = null;

		            if (descriptor instanceof IndexedPropertyDescriptor)
		            {
		            	int length = entry.getValue().asJsonList().size();
		            	Object A = Array.newInstance(((IndexedPropertyDescriptor) descriptor).getIndexedPropertyType(), 
		            						length);
		            	for (int i = 0; i < length; i++)
		            		Array.set(A, i, from(entry.getValue().at(i)));
		            	
		            }
		            else if (propertyClass.equals(Class.class))
		            	// TODO: what about thread class loader?
		            	value = Class.forName(entry.getValue().asString());
		            else
		            	value = value(entry.getValue());
		            BonesOfBeans.setProperty(bean, entry.getKey(), value);
		        }
				return bean;
			}
			catch (Exception ex)
			{
				HGUtils.throwRuntimeException(ex);
				return null;
			}
		}
	};
	
	BeanJsonConverter beanConverter = new BeanJsonConverter();
	
	public Json nil()
	{
		return f.nil();
	}

	public Json bool(boolean value)
	{
		return f.bool(value);
	}

	public Json string(String value)
	{
		return f.string(value);
	}

	public Json number(Number value)
	{
		return f.number(value);
	}

	public Json object()
	{
		return f.object();
	}

	public Json array()
	{
		return f.array();
	}

	public Json make(Object anything)
	{
		if (anything == null)
			return Json.nil();
		else if (anything instanceof Performative)
			return f.make(anything.toString());

		boolean isarray = anything.getClass().isArray();
		Class<?> type = isarray ? anything.getClass().getComponentType() : anything.getClass();
		JsonConverter converter = converterMap.get(type.getName());
		String typeName = shortNameMap.getY(type.getName());
		if (typeName == null)
			typeName = type.getName();
		if (isarray)
		{
			Json result = Json.array();
			Object[] A = (Object[])anything;
			for (Object x : A)
			{
				if (x == null)
					result.add(Json.nil());
				else
					result.add(converter != null ? converter.to(x) : make(x));
			}
			return Json.object("javaArrayType", typeName, "array", result);
		}
		else if (type.isEnum())
			return Json.object("java.lang.Enum", type.getName(), "value", anything.toString());
		else if (converter != null)
			return Json.object("javaType", typeName, "value", converter.to(anything));
		
		Json j = null;
		try { j = f.make(anything); } catch (Throwable t) { }
		if (j != null)
			return j;		
		else
			return Json.object("javaType", typeName, "value", beanConverter.to(anything));
	}
	
	@SuppressWarnings("unchecked")
	public <T> T value(Json x)
	{
		if (x == null || x.isNull())
			return null;
		else if (x.isPrimitive())
			return (T)x.getValue();
		else if (x.isArray())
			// what to do here ... this is some sort of collection??
			return (T)x.getValue();
		else if (x.has("java.lang.Enum"))
		{
			try
			{
				return (T) Enum.valueOf((Class<Enum>)Class.forName(x.at("java.lang.Enum").asString()), 
							x.at("value").asString());
			}
			catch (Exception t)
			{
				throw new RuntimeException(t);
			}
		}
		else if (x.has("javaArrayType"))
		{
			String fullName = shortNameMap.getX(x.at("javaArrayType").asString());
			if (fullName == null) fullName = x.at("javaArrayType").asString();
			try
			{
				Class<?> cl = Class.forName(fullName);
				JsonConverter converter = converterMap.get(fullName);
				if (converter == null)
					converter = beanConverter;
				Object A = Array.newInstance(cl, x.at("array").asJsonList().size());
				for (int i = 0; i < x.at("array").asJsonList().size(); i++)
				{
					Json j = x.at("array").at(i);
					Array.set(A, i, j.isNull() ? null : converter.from(j));
				}
				return (T)A;
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
		}
		else if (x.has("javaType"))
		{
			String fullName = shortNameMap.getX(x.at("javaType").asString());
			if (fullName == null)
				fullName = x.at("javaType").asString();
			JsonConverter converter = converterMap.get(fullName);
			if (converter != null)
				return (T)converter.from(x.at("value"));
			else
				return (T)beanConverter.from(x.at("value"));
		}
		else
			return (T)x.getValue();
	}
}