package org.hypergraphdb.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

/**
 * <p>
 * Represents version information at a particular database instance. This is a plain
 * Java property file. For each component the corresponding version is stored as a 
 * property. The core component is <code>hgdb</code>, so to obtain the version of the
 * HyperGraphDB in directory <code>dir</code>, call 
 * <code>HGEnvironment.getVersions(dir).getVersion("hgdb")</code>.  
 * </p>
 * 
 * @author Borislav Iordanov
 * @since
 */
public class HGDatabaseVersionFile 
{
	private File file;
	private Properties versions = null;
	
	private synchronized void ensureLoaded()
	{
		if (versions != null)
			return;
		FileInputStream in = null;
		try
		{
			versions = new Properties();
			if (!file.exists())
				return;
			in = new FileInputStream(file);
			versions.load(in);
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
		finally
		{
			if (in != null) try { in.close(); } catch (Throwable t) { }
		}
	}
	
	private synchronized void save()
	{
		FileOutputStream out = null;
		try
		{
			out = new FileOutputStream(file);
			versions.store(out, null);
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
		finally
		{
			if (out != null) try { out.close(); } catch (Throwable t) { }
		}
	}
	
	public HGDatabaseVersionFile(File file)
	{
		this.file = file;
	}
	
	public File getFile()
	{
		return file;
	}
	
	public void setVersion(String component, String version)
	{
		ensureLoaded();
		versions.put(component, version);
		save();
	}
	
	public String getVersion(String component)
	{
		ensureLoaded();
		return versions.getProperty(component);
	}
}
