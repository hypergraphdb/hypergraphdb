package org.hypergraphdb.storage.mdbx;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;
import org.hypergraphdb.HGConfiguration;


import com.castortech.mdbxjni.DatabaseConfig;
import com.castortech.mdbxjni.EnvConfig;

public class MdbxConfig
{
	private final Properties mdbxProps;

	private static final String CONFIG_PREFIX = "irisconfig.";
	private static final String PROP_FILENAME = "/config/mdbx-config.properties"; //$NON-NLS-1$ //NOSONAR:
																					// fine
																					// as
																					// this
																					// is
																					// a
																					// fallback
																					// value

//	private HGConfiguration hgConfig;
	private EnvConfig envConfig;
	private DatabaseConfig dbConfig;

	private int syncFrequency = 0;
	private boolean syncForce = false;

	/**
	 * The static default properties of MDBX.
	 */
	@SuppressWarnings("nls")
	static Properties getDefaultMdbxProperties()
	{
		Properties properties = new Properties();

		// These properties have no default and are required, so those are our
		// defaults
		properties.setProperty("map.size", Long.toString(2000L * 1024 * 1024)); // 2GB
																				// default
		properties.setProperty("max.readers", Integer.toString(300));
		properties.setProperty("max.dbs", Long.toString(30));

		// these are internal
		properties.setProperty("sync.frequency", Integer.toString(0));
		properties.setProperty("sync.force", Boolean.FALSE.toString());

		return properties;
	}

	public MdbxConfig(HGConfiguration hgConfig)
	{
//		this.hgConfig = hgConfig;

		mdbxProps = new Properties(getDefaultMdbxProperties())
		{
			private static final long serialVersionUID = 1L;

			@Override
			public synchronized Object put(Object key, Object value)
			{
				Object newVal = value; // EvalUtils.eval((String) value);
				return super.put(key, newVal.toString());
			}
		};

		final InputStream is = MdbxConfig.class.getResourceAsStream(PROP_FILENAME);

		if (is != null)
		{
			try
			{
				mdbxProps.load(is);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		// Add support for system properties used to redefine the configuration
		Properties sysProperties = System.getProperties();
		String keyPrefix = "mdbx."; //hgConfig.getDbConfigKeyPrefix();
		Enumeration<?> propertyNames = sysProperties.propertyNames();
		while (propertyNames.hasMoreElements())
		{
			String key = (String) propertyNames.nextElement();
			if (key.startsWith(keyPrefix))
			{
				mdbxProps.setProperty(key.replaceFirst(keyPrefix, ""),
						sysProperties.getProperty(key));
			}

			if (key.startsWith(CONFIG_PREFIX + keyPrefix))
			{
				mdbxProps.setProperty(
						key.replaceFirst(CONFIG_PREFIX + keyPrefix, ""),
						sysProperties.getProperty(key));
			}
		}
		envConfig = new EnvConfig();
		dbConfig = new DatabaseConfig();
		applyConfig();
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

	@SuppressWarnings({ "nls", "deprecation" })
	private void applyConfig()
	{
		EnvConfig defaultConfig = new EnvConfig();
		envConfig.setReadOnly(false);
		// envConfig.setReadOnly(Boolean.parseBoolean(mdbxProps.getProperty("read.only")));

		// These properties set the db geometry (sizing), growth is required if
		// db is allowed to grow in size
		envConfig.setMapLower(Long.parseLong(mdbxProps.getProperty(
				"map.size.lower", Long.toString(defaultConfig.getMapLower()))));
		envConfig.setMapSize(Long.parseLong(mdbxProps.getProperty("map.size",
				Long.toString(defaultConfig.getMapSize()))));
		envConfig.setMapUpper(Long.parseLong(mdbxProps.getProperty(
				"map.size.upper", Long.toString(defaultConfig.getMapUpper()))));
		envConfig.setMapGrowth(
				Long.parseLong(mdbxProps.getProperty("map.size.growth",
						Long.toString(defaultConfig.getMapGrowth()))));
		envConfig.setMapShrink(
				Long.parseLong(mdbxProps.getProperty("map.size.shrink",
						Long.toString(defaultConfig.getMapShrink()))));
		envConfig.setPageSize(Long.parseLong(mdbxProps.getProperty("page.size",
				Long.toString(defaultConfig.getPageSize()))));

		// These properties have no default and are required
		envConfig.setMaxDbs(Long.parseLong(mdbxProps.getProperty("max.dbs")));
		envConfig.setMaxReaders(
				Integer.parseInt(mdbxProps.getProperty("max.readers")));

		// Properties with defaults
		envConfig.setCoalesce(Boolean.parseBoolean(mdbxProps.getProperty(
				"coalesce", Boolean.toString(defaultConfig.isCoalesce()))));
		envConfig.setLifoReclaim(
				Boolean.parseBoolean(mdbxProps.getProperty("lifo.reclaim",
						Boolean.toString(defaultConfig.isLifoReclaim()))));
		envConfig.setMapAsync(Boolean.parseBoolean(mdbxProps.getProperty(
				"map.async", Boolean.toString(defaultConfig.isMapAsync()))));
		envConfig.setMode(Integer.parseInt(mdbxProps.getProperty("file.mode",
				Integer.toString(defaultConfig.getMode()))));
		envConfig.setNoMemInit(Boolean.parseBoolean(mdbxProps.getProperty(
				"no.mem.init", Boolean.toString(defaultConfig.isNoMemInit()))));
		envConfig.setNoMetaSync(
				Boolean.parseBoolean(mdbxProps.getProperty("no.meta.sync",
						Boolean.toString(defaultConfig.isNoMetaSync()))));
		envConfig.setNoReadAhead(
				Boolean.parseBoolean(mdbxProps.getProperty("no.read.ahead",
						Boolean.toString(defaultConfig.isNoReadAhead()))));
		envConfig.setNoSubDir(Boolean.parseBoolean(mdbxProps.getProperty(
				"no.subdir", Boolean.toString(defaultConfig.isNoSubDir()))));
		envConfig
				.setNoSync(Boolean.parseBoolean(mdbxProps.getProperty("no.sync",
						Boolean.toString(defaultConfig.isNoSync()))));
		envConfig.setNoTLS(Boolean.parseBoolean(mdbxProps.getProperty("no.tls",
				Boolean.toString(defaultConfig.isNoTLS()))));
		envConfig.setPagePerturb(
				Boolean.parseBoolean(mdbxProps.getProperty("page.perturb",
						Boolean.toString(defaultConfig.isPagePerturb()))));
		envConfig.setUtterlyNoSync(
				Boolean.parseBoolean(mdbxProps.getProperty("utterly.no.sync",
						Boolean.toString(defaultConfig.isUtterlyNoSync()))));
		envConfig.setWriteMap(Boolean.parseBoolean(mdbxProps.getProperty(
				"write.map", Boolean.toString(defaultConfig.isWriteMap()))));

		// settings for the pooled cursors, need at least pooled.cursors set to
		// true to activate
		envConfig.setUsePooledCursors(
				Boolean.parseBoolean(mdbxProps.getProperty("pooled.cursors",
						Boolean.toString(defaultConfig.isUsePooledCursors()))));
		Optional.ofNullable(
				mdbxProps.getProperty("pooled.cursor.eviction.time.secs"))
				.map(Integer::parseInt)
				.ifPresent(secs -> envConfig
						.setPooledCursorTimeBetweenEvictionRuns(
								Duration.ofSeconds(secs)));
		envConfig.setPooledCursorMaxIdle(Integer.parseInt(mdbxProps.getProperty(
				"pooled.cursor.max.idle",
				Integer.toString(defaultConfig.getPooledCursorMaxIdle()))));
		Optional.ofNullable(mdbxProps
				.getProperty("pooled.cursor.eviction.min.idle.time.secs"))
				.map(Integer::parseInt).ifPresent(
						secs -> envConfig.setPooledCursorMinEvictableIdleTime(
								Duration.ofSeconds(secs)));
		envConfig.setPooledCloseMaxWaitSeconds(Integer.parseInt(mdbxProps
				.getProperty("pooled.close.max.wait.secs", Integer.toString(
						defaultConfig.getPooledCloseMaxWaitSeconds()))));

		dbConfig.setCreate(true);

		// these are not related to envConfig
		syncFrequency = Integer
				.parseInt(mdbxProps.getProperty("sync.frequency"));
		syncForce = Boolean.parseBoolean(mdbxProps.getProperty("sync.force"));
	}

	@SuppressWarnings("deprecation")
	public String toStringDetail()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("Environment Config [");
		sb.append("readOnly:");
		sb.append(envConfig.isReadOnly());
		sb.append(", mapLower:");
		sb.append(envConfig.getMapLower());
		sb.append(", mapSize:");
		sb.append(envConfig.getMapSize());
		sb.append(", mapUpper:");
		sb.append(envConfig.getMapUpper());
		sb.append(", mapGrowth:");
		sb.append(envConfig.getMapGrowth());
		sb.append(", mapShrink:");
		sb.append(envConfig.getMapShrink());
		sb.append(", pageSize:");
		sb.append(envConfig.getPageSize());
		sb.append(", maxDbs:");
		sb.append(envConfig.getMaxDbs());
		sb.append(", maxReaders:");
		sb.append(envConfig.getMaxReaders());
		sb.append(", coalesce:");
		sb.append(envConfig.isCoalesce());
		sb.append(", lifoReclaim:");
		sb.append(envConfig.isLifoReclaim());
		sb.append(", mapAsync:");
		sb.append(envConfig.isMapAsync());
		sb.append(", fileMode:");
		sb.append(envConfig.getMode());
		sb.append(", noMemInit:");
		sb.append(envConfig.isNoMemInit());
		sb.append(", noMetaSync:");
		sb.append(envConfig.isNoMetaSync());
		sb.append(", noReadAhead:");
		sb.append(envConfig.isNoReadAhead());
		sb.append(", noSubDir:");
		sb.append(envConfig.isNoSubDir());
		sb.append(", noSync:");
		sb.append(envConfig.isNoSync());
		sb.append(", noTLS:");
		sb.append(envConfig.isNoTLS());
		sb.append(", pagePerturb:");
		sb.append(envConfig.isPagePerturb());
		sb.append(", utterlyNoSync:");
		sb.append(envConfig.isUtterlyNoSync());
		sb.append(", writeMap:");
		sb.append(envConfig.isWriteMap());
		sb.append("], SyncFrequency:");
		sb.append(syncFrequency);
		sb.append(", SyncForce:");
		sb.append(syncForce);
		return sb.toString();
	}
}