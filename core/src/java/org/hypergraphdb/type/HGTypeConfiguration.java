package org.hypergraphdb.type;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
    private HGTypeSchema<?> defaultSchema = new JavaTypeSchema();
    private Map<String, HGTypeSchema<?>> schemas = new HashMap<String, HGTypeSchema<?>>();    

    public HGTypeConfiguration()
    {
        setDefaultSchema(defaultSchema);
    }    
    
    public Collection<HGTypeSchema<?>> getSchemas()
    {
        return schemas.values();
    }
    
    /**
     * <p>Return the instance responsible for creating HyperGraphDB type from Java classes.</p>
     */
    @SuppressWarnings("unchecked")
    public <T extends HGTypeSchema<?>> T getDefaultSchema()
    {
        return (T)this.defaultSchema;
    }

    /**
     * <p>Specify the instance responsible for creating HyperGraphDB type from Java classes.</p>
     */    
    public void setDefaultSchema(HGTypeSchema<?> typeSchema)
    {
        this.defaultSchema = typeSchema;
        schemas.put(typeSchema.getName(), typeSchema);
    }
    
    public void addSchema(HGTypeSchema<?>...schemas)
    {
        for (HGTypeSchema<?> s : schemas)
            this.schemas.put(s.getName(), s);
    }
    
    @SuppressWarnings("unchecked")
    public <T extends HGTypeSchema<?>> T getSchema(String name)
    {
        return (T)schemas.get(name);
    }
}