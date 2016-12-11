package org.hypergraphdb.storage.lmdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.fusesource.lmdbjni.DatabaseConfig;
import org.fusesource.lmdbjni.EnvConfig;

public class LmdbConfig
{
	private final Properties lmdbProps;
	private static final String propFileName = "/config/lmdb-config.properties"; //$NON-NLS-1$

	private EnvConfig envConfig;
	private DatabaseConfig dbConfig;

	private int syncFrequency = 0;
	private boolean syncForce = false;

	/**
	 * The static default properties of LMDB.
	 */
	@SuppressWarnings("nls")
	static Properties getDefaultLmdbProperties()
	{
		Properties properties = new Properties();
		properties.setProperty("map.size",
				new Long(2000L * 1024 * 1024).toString()); // 2GB default
		properties.setProperty("max.readers", new Integer(-1).toString());
		properties.setProperty("max.dbs", new Long(30).toString());
		properties.setProperty("no.sync", new Boolean(false).toString());
		properties.setProperty("no.meta.sync", new Boolean(false).toString());
		properties.setProperty("map.async", new Boolean(false).toString());
		properties.setProperty("write.map", new Boolean(false).toString());
		properties.setProperty("sync.frequency", new Integer(0).toString());
		properties.setProperty("sync.force", new Boolean(false).toString());

		return properties;
	}

	@SuppressWarnings("nls")
	private void resetDefaults()
	{
		envConfig.setReadOnly(false);
		envConfig.setMapSize(Long.parseLong(lmdbProps.getProperty("map.size")));
		envConfig.setMaxDbs(Long.parseLong(lmdbProps.getProperty("max.dbs")));
		envConfig.setMaxReaders(Long.parseLong(lmdbProps.getProperty("max.readers")));
		envConfig.setNoSync(Boolean.parseBoolean(lmdbProps.getProperty("no.sync")));
		envConfig.setNoMetaSync(Boolean.parseBoolean(lmdbProps.getProperty("no.meta.sync")));
		envConfig.setWriteMap(Boolean.parseBoolean(lmdbProps.getProperty("write.map")));
		envConfig.setMapAsync(Boolean.parseBoolean(lmdbProps.getProperty("map.async")));
		envConfig.setWriteMap(Boolean.parseBoolean(lmdbProps.getProperty("write.map")));
		dbConfig.setCreate(true);

		syncFrequency = Integer.parseInt(lmdbProps.getProperty("sync.frequency"));
		syncForce = Boolean.parseBoolean(lmdbProps.getProperty("sync.force"));
	}

	public LmdbConfig()
	{
		lmdbProps = new Properties(getDefaultLmdbProperties())
		{
			private static final long serialVersionUID = 1L;

			public synchronized Object put(Object key, Object value)
			{
				ScriptEngineManager mgr = new ScriptEngineManager();
				ScriptEngine engine = mgr.getEngineByName("JavaScript");
				String val = (String) value;
				Object newVal = val;

				try
				{
					newVal = engine.eval(val);
					if (newVal instanceof Double)
					{
						newVal = Long.toString(((Double) newVal).longValue());
					}
					else
					{ // converted possibly to class, package or whatever.
						newVal = val;
					}
				}
				catch (ScriptException e1)
				{
				}

				return super.put(key, newVal.toString());
			}
		};

		final InputStream is = LmdbConfig.class
				.getResourceAsStream(propFileName);

		if (is != null)
		{
			try
			{
				lmdbProps.load(is);
			}
			catch (IOException e)
			{
				System.err.println(
						"Error loading LMDB config properties. Will use built-in defaults ONLY");
				e.printStackTrace();
			}
		}
		else
		{
			System.err.println(
					"Error finding LMDB config properties. Will use built-in defaults ONLY");
		}

		// Add support for system properties used to redefine the configuration
		Properties sysProperties = System.getProperties();
		Enumeration<?> propertyNames = sysProperties.propertyNames();
		while (propertyNames.hasMoreElements())
		{
			String key = (String) propertyNames.nextElement();
			if (key.startsWith("lmdb."))
			{
				lmdbProps.setProperty(key.replaceAll("lmdb.", ""),
						sysProperties.getProperty(key));
			}
		}
		envConfig = new EnvConfig();
		dbConfig = new DatabaseConfig();
		resetDefaults();
	}

	public EnvConfig getEnvironmentConfig()
	{
		return envConfig;
	}

	public DatabaseConfig getDatabaseConfig()
	{
		return dbConfig;
	}

	public int getSyncFrequency()
	{
		return syncFrequency;
	}

	public void setSyncFrequency(int syncFrequency)
	{
		this.syncFrequency = syncFrequency;
	}

	public boolean isSyncForce()
	{
		return syncForce;
	}

	public void setSyncForce(boolean syncForce)
	{
		this.syncForce = syncForce;
	}
}