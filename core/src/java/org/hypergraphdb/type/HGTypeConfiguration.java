package org.hypergraphdb.type;

/**
 * <p>
 * This class encapsulates startup configuration parameters for the HyperGraphDB
 * type system. An instance of this class is provided in the top-level
 * {@link HGConfiguration} 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGTypeConfiguration
{
    private String predefinedTypes = "/org/hypergraphdb/types";
    private JavaTypeMapper javaTypeMapper = new JavaTypeFactory();
    
    /**
     * <p>Return the location of the type configuration file. This file can be either 
     * a classpath resource or a file on disk or 
     */
    public String getPredefinedTypes()
    {
        return predefinedTypes;
    }

    /**
     * <p>
     * Specify the type configuration file to use when bootstrapping the type system. This file
     * must contain the list of predefined types needed for the normal functioning of a database
     * instance. Each line in this text file is a space separated list of (1) the persistent handle
     * of the type (2) The Java class implementing the {@link HGAtomType} interface and optionally
     * (3) one or more Java classes to which the type implementation is associated. 
     * </p>
     * 
     * @param typeConfiguration The location of the type configuration file. First, an attempt
     * is made to load this location is a classpath resource. Then as a local file. Finally as
     * a remote URL-based resource. 
     */
    public void setPredefinedTypes(String predefinedTypes)
    {
        this.predefinedTypes = predefinedTypes;
    }

    /**
     * <p>Return the instance responsible for creating HyperGraphDB type from Java classes.</p>
     */
    public JavaTypeMapper getJavaTypeMapper()
    {
        return javaTypeMapper;
    }

    /**
     * <p>Specify the instance responsible for creating HyperGraphDB type from Java classes.</p>
     */    
    public void setJavaTypeMapper(JavaTypeMapper javaTypeMapper)
    {
        this.javaTypeMapper = javaTypeMapper;
    }    
}