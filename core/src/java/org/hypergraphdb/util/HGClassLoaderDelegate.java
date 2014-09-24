package org.hypergraphdb.util;

import org.hypergraphdb.HGConfiguration;

public class HGClassLoaderDelegate extends ClassLoader
{
	private HGConfiguration config;
	
	public HGClassLoaderDelegate(HGConfiguration config)
	{
		this.config = config;
	}
	
	@Override
	protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException
	{
		Class<?> cl = HGUtils.getClassLoader(config).loadClass(name);
		if (resolve)
			super.resolveClass(cl);
		return cl;
	}
}