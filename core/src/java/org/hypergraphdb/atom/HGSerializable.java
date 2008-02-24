package org.hypergraphdb.atom;

/**
 * 
 * <p>
 * This atom marks a Java class (or interface) for serialization in HyperGraph 
 * storage. More concretely, all its sub-types (interfaces or classes) will 
 * be mapped to HyperGraph types to work directly with the private fields 
 * similarly to what standard Java serialization thus, as opposed to accessible
 * object property through the Java beans getter/setter idiom. However, unlike
 * standard Java serialization which is binary, the HyperGraph representation still 
 * relies on records and slots, thus making the data indexable and searcheable, albeit
 * with the ensuing slower instantiation time.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGSerializable
{
	private String classname;
	
	public HGSerializable()
	{		
	}

	public HGSerializable(String classname)
	{
		this.classname = classname;
	}
	
	public String getClassname()
	{
		return classname;
	}

	public void setClassname(String classname)
	{
		this.classname = classname;
	}
}
