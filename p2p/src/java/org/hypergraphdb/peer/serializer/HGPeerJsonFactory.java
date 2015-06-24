package org.hypergraphdb.peer.serializer;

import java.beans.IndexedPropertyDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import mjson.Json;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.PhantomHandle;
import org.hypergraphdb.handle.PhantomManagedHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.handle.WeakHandle;
import org.hypergraphdb.handle.WeakManagedHandle;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.query.Or;
import org.hypergraphdb.query.TypedValueCondition;
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
	Map<Class<?>, JsonConverter> converterFromAbstractMap = new ConcurrentHashMap<Class<?>, JsonConverter>();
	
	static HGPeerJsonFactory instance = new HGPeerJsonFactory();
	static
	{
		instance.converterMap.put(UUID.class.getName(), instance.uuidConverter);
		instance.converterMap.put(And.class.getName(), instance.collectionConverter);
		instance.converterMap.put(Or.class.getName(), instance.collectionConverter);
		instance.converterMap.put(PhantomManagedHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(PhantomHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(WeakHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(WeakManagedHandle.class.getName(), instance.handleConverter);
		instance.converterMap.put(UUIDPersistentHandle.class.getName(), instance.handleConverter);		
		instance.converterMap.put(Constant.class.getName(), instance.constantConverter);
		instance.converterMap.put(Class.class.getName(), instance.classConverter);
		instance.converterMap.put(Pattern.class.getName(), instance.regexConverter);
		instance.converterMap.put(AtomValueCondition.class.getName(), 
				instance.new BeanJsonConverter(new String[] {"operator", "valueReference"}));
		instance.converterMap.put(AtomTypeCondition.class.getName(), 
				instance.new BeanJsonConverter(new String[] {"typeReference"}));
		instance.converterMap.put(TypedValueCondition.class.getName(), 
				instance.new BeanJsonConverter(new String[] {"operator", "valueReference", "typeReference"}));
		instance.converterMap.put(HGIndex.class.getName(), instance.indexConverter);		
		instance.converterFromAbstractMap.put(HGIndex.class, instance.indexConverter);
	}
	
	Number castNumber(Class<?> cl, Json v)
	{
	    if (!v.isNumber())
	        throw new IllegalArgumentException("Trying to cast " + v + 
	                " as a Number bean property, but it's not a number.");
	    if (cl.equals(short.class) || cl.equals(Short.class))
	        return v.asShort();
	    else if (cl.equals(byte.class) || cl.equals(Byte.class))
            return v.asByte();
        else if (cl.equals(int.class) || cl.equals(Integer.class))
            return v.asInteger();
        else if (cl.equals(float.class) || cl.equals(Float.class))
            return v.asFloat();
        else if (cl.equals(long.class) || cl.equals(Long.class))
            return v.asLong();
        else if (cl.equals(BigDecimal.class))
            return new BigDecimal(v.asDouble());
        else if (cl.equals(BigInteger.class))
            return BigInteger.valueOf(v.asLong());
        else if (cl.equals(AtomicInteger.class))
            return new AtomicInteger(v.asInteger());	    
        else if (cl.equals(AtomicLong.class))
            return new AtomicLong(v.asLong());     
	    else
	        return v.asDouble();
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
		@SuppressWarnings("rawtypes")
		public Json to(Object x) 
		{			
			return makeTyped(((Constant)x).get()); 
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Object from(Json x) 
		{
			try 
			{
/*				Class<?> cl = Class.forName(x.at("javaType").asString());
				if (Number.class.isAssignableFrom(cl))
				{
					// we're instantiating from a string to avoid an if-then-else for each
					// every Java primitive number type
					return new Constant<Object>(
							cl.getConstructor(String.class).newInstance(
									x.at("value").getValue().toString()));
				}
				else */
					return new Constant(value(x));	
			} 
			catch (Exception ex)
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

	public final JsonConverter regexConverter = new JsonConverter()
	{
		public Json to(Object x) { return Json.make(((Pattern)x).toString()); }
		public Object from(Json x) { return Pattern.compile(x.asString()); }		
	};

	public final JsonConverter indexConverter = new JsonConverter()
	{
		public Json to(Object x) { return Json.object().set("javaType", HGIndex.class.getName())
										.set("value", ((HGIndex<?,?>)x).getName()); }
		public Object from(Json x) { return graph.getStore().getIndex(x.asString()); }		
	};
	
	public final JsonConverter classConverter = new JsonConverter()
	{
		public Json to(Object x) 
		{ 
			return Json.make(((Class<?>)x).getName()); 
		}
		public Object from(Json x) 
		{ 
			try
			{
				return HGUtils.loadClass(graph, x.asString());
			}
			catch (ClassNotFoundException e)
			{
				throw new HGException(e);
			} 
		}		
	};
	
	public final JsonConverter collectionConverter = new JsonConverter()
	{
		public Json to(Object x) 
		{ 
			Json result = Json.object().set("javaType", x.getClass().getName()).set("data", array());
			for (Object item : (Collection<?>)x)
				result.at("data").add(make(item));
			return result;
		}
		@SuppressWarnings("unchecked")
		public Object from(Json x) 
		{ 
			Collection<Object> C;
			try
			{
				C = (Collection<Object>)HGUtils.loadClass(graph, x.at("javaType").asString()).newInstance();
				for (Json j : x.at("data").asJsonList())
					C.add(value(j));
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			return C;
		}		
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
		boolean ignoreNulls = false;
		String[] props;
		public BeanJsonConverter() { }
		public BeanJsonConverter(String [] props) { this.props = props; }
		
		private Json makeProperty(Object value)
		{
			if (value == null)
				return Json.nil();
			Class<?> type = value.getClass();
			if (Number.class.isAssignableFrom(type))
				return makeTyped(value);
			else
				return make(value);
		}
		
		public Json to(Object x)
		{
			String typeName = shortNameMap.containsX(x.getClass().getName()) ?
							  shortNameMap.getY(x.getClass().getName()) : x.getClass().getName();
            Json result = Json.object().set("javaType", typeName);
            if (x instanceof HGLink)
            {
            	result.set("_link", array());
            	for (int i = 0; i < ((HGLink)x).getArity(); i++)
            		result.at("_link").add(((HGLink)x).getTargetAt(i));
            }
            if (x instanceof Collection)
            {
            	result.set("_collection", array());
            	for (Object element : (Collection<?>)x)
            		result.at("_collection").add(makeProperty(element));            	
            }
            if (x instanceof Map)
            {
            	result.set("_map", array());
            	for (Map.Entry<?, ?> e : ((Map<?,?>)x).entrySet())
            		result.at("_map").add(Json.object().set("key", makeProperty(e.getKey())) 
            										   .set("value", makeProperty(e.getValue())));            	            	
            }
            try
            {
                if (props != null) // user provided list of properties
                    for (String propname : props)
                    {
                        PropertyDescriptor desc = BonesOfBeans.getPropertyDescriptor(x,
                                                                                     propname);
                        Object value = BonesOfBeans.getProperty(x, desc);
                        if (!ignoreNulls || value != null)
                        	result.set(propname, makeProperty(value));
                    }
                else // all introspected properties
                    for (PropertyDescriptor desc : BonesOfBeans.getAllPropertyDescriptors(x)
                            .values())
                        if (desc.getReadMethod() != null
                                && desc.getWriteMethod() != null)
                        {
                            Object value = BonesOfBeans.getProperty(x, desc);
                            if (!ignoreNulls || value != null)                        	
                            	result.set(desc.getName(), makeProperty(value));
                        }
                return result;
            }
            catch (Throwable ex)
            {
                HGUtils.throwRuntimeException(ex);
            }
            return null; // unreachable			
		}
		
		@SuppressWarnings("unchecked")
		public Object from(Json x)
		{
			if (!x.isObject())
				return x.getValue();
			Json typeName = x.at("javaType");
			if ("mjson.Json".equals(typeName))
				return x.at("value");
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
			
			if (fullName.equals(String.class.getName()))
				return x.at("value").getValue();
			else if (fullName.equals(Boolean.class.getName()))
				return x.at("value").getValue();
			
			try
			{
				Class<?> beanClass = HGUtils.loadClass(graph, fullName);
				if (Number.class.isAssignableFrom(beanClass))
					return castNumber(beanClass, x.at("value"));
					//return beanClass.getConstructor(new Class[]{String.class}).newInstance(x.at("value").getValue().toString());
				else if (Boolean.class.isAssignableFrom(beanClass))
					return x.at("value").asBoolean();
				Object bean = null;
				x = x.at("value");
				if (HGLink.class.isAssignableFrom(beanClass) && x.has("_link"))
				{
					HGHandle [] targets = new HGHandle[x.at("_link").asJsonList().size()];
					for (int i = 0; i < targets.length; i++)
						targets[i] = value(x.at("_link").at(i));
					bean = beanClass.getDeclaredConstructor(new Class[] {HGHandle[].class} ).newInstance(new Object[]{targets});
				}
				else
					bean = beanClass.newInstance();
	            if (x.has("_collection"))
	            	for (Json element : x.at("_collection").asJsonList())
	            		((Collection<Object>)bean).add(value(element));            	
	            if (x.has("_map"))
	            	for (Json entry : x.at("_map").asJsonList())
	            		((Map<Object,Object>)bean).put(value(entry.at("key")), value(entry.at("value")));            	            					
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
		            {
		            	if (entry.getValue().isString())
		            		value = HGUtils.loadClass(graph, entry.getValue().asString());
		            	else if (!entry.getValue().isNull())
		            		value = HGUtils.loadClass(graph, entry.getValue().at("value").asString());
		            }
		            else if (entry.getValue().isNumber())
		                value = castNumber(propertyClass, entry.getValue());
		            else
		            	value = value(entry.getValue());
		            BonesOfBeans.setProperty(bean, entry.getKey(), value);
		        }
				return bean;
			}
			catch (Exception ex)
			{
				throw new RuntimeException("Failed to JSON-deserialize bean of type " + fullName, ex);
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

	public Json makeTyped(Object anything)
	{
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
			return Json.object().set("javaArrayType", typeName).set("array", result);
		}
		else if (type.isEnum())
			return Json.object().set("java.lang.Enum", type.getName()).set("value", anything.toString());
		
		for (Class<?> abstractConv : converterFromAbstractMap.keySet())
			if (abstractConv.isAssignableFrom(type))
				return converterFromAbstractMap.get(abstractConv).to(anything);
		Json value = null;
		if (converter != null)
			value = converter.to(anything); 
		else if (Collection.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type))
			value = beanConverter.to(anything);
		else try 
		{ 
			value = f.make(anything); 
		} 
		catch (Throwable t) 
		{ 
			value = beanConverter.to(anything);
		}
		return Json.object().set("javaType", typeName).set("value", value);		
	}
	
	public Json make(Object anything)
	{
		if (anything == null)
			return Json.nil();
		else if (anything instanceof String)
			return f.string((String)anything);
		else if (anything instanceof Boolean)
			return f.bool((Boolean)anything);
		else if (anything instanceof Number)
			return f.number((Number)anything);
		else if (anything instanceof Json)
			return (Json)anything;
		else if (anything instanceof Performative)
			return f.make(anything.toString());
		
		return makeTyped(anything);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
				Object A = Array.newInstance(cl, x.at("array").asJsonList().size());
				for (int i = 0; i < x.at("array").asJsonList().size(); i++)
				{
					Json j = x.at("array").at(i);
					Array.set(A, i, j.isNull() ? null : converter != null ? converter.from(j) : value(j));
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
				return (T)converter.from(converter instanceof BeanJsonConverter ? x : x.at("value"));
			else
				return (T)beanConverter.from(x); // .at("value"));
		}
		else
			return (T)x.getValue();
	}
}