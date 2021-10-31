package org.hypergraphdb.app.management;

import static org.hypergraphdb.util.HGUtils.*;

/**
 * 
 * <p>
 * A <code>HGApplication</code> represents a set of HyperGraph atoms and 
 * supporting APIs that model a particular domain, or implement a particular
 * model in the context of HyperGraph. As usual, applications are software
 * components that have a name and a version and possibly dependencies on
 * other such applications. They follow a certain lifecycle within the host system:
 * they are being installed, uninstalled and updated. 
 * The term <code>component</code> could have been used instead of <code>application</code>, 
 * but the latter seems to better reflect the intended level of granularity. An 
 * application is typically something that is distributed in a jar and contains several
 * "components". That's the intended use, anyways.
 * </p>
 *
 * <p>
 * The version of an application is arbitrary string that is be defautl lexicographically
 * compared which covers most common versionning schema. The <code>compareVersion</code>
 * can be overriden to implement specific versionning schemas. The framework only cares
 * about the order of versionned applications (for things like insuring compatibility etc.).
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public abstract class HGApplication implements PresenceLifecycle
{
	private String name;
	private String version;
	
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public String getVersion()
	{
		return version;
	}
	public void setVersion(String version)
	{
		this.version = version;
	}

	/**
	 * <p>Return a number that is < 0, = 0, > 0 depending on whether
	 * is the version of this object is less than, equal to or greater 
	 * than the version of the passed in object.</p>
	 */
	public int compareVersions(HGApplication other)
	{
		return version.compareTo(other.version);
	}
	
	public boolean equals(Object other)
	{
		if (! (other instanceof HGApplication))
			return false;
		HGApplication app = (HGApplication)other;
		return eq(name, app.name) && eq(version, app.version);  
	}
	
	public int hashCode()
	{
		return hashThem(name, version);
	}
}