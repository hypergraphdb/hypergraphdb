package org.hypergraphdb.conv;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public interface Converter {

	/*
	 *Return a map with all slots needed for this converter  
	 */
	public Map<String, Class<?>> getSlots();

	/*
	 * Construct the suitable object from supplied properties 
	 */
	public Object make(Map<String, Object> props);

	/*
	 * Store the supplied instance in a map
	 */
	public Map<String, Object> store(Object instance);

	public Set<AddOnType> getAddOnFields();
	public Constructor<?> getCtr();
	public String[] getCtrArgs();
	public Method getFactoryCtr();
	public void setCtr(Constructor<?> ctr);
	public void setFactoryCtr(Class<?> cls, String method, String[] paramNames,
			Class<?>[] paramTypes);
	
	static interface AddOnType{
		public Class<?>[] getTypes();
		public String getName() ;
		public String[] getArgs() ;
	}
	
	static class Add implements AddOnType
	{
		private String relName;
		private Class<?>[] types;
		private String[] args;
		
		public Add(String relName, String[] args, Class<?>[] types) {
			this.relName = relName;
			this.types = types;
			this.args = args;
		}

		public Class<?>[] getTypes()
		{
			return types;
		}

		public String getName() {
			return relName;
		}

		public String[] getArgs() {
			return args;
		}
		
//		public boolean equals(Object o)
//		{
//			if(!(o instanceof AddOnType))
//				return false;
//			AddOnType on = (AddOnType)o;
//			 
//		}
	}
}


