package org.hypergraphdb.type;

import java.net.URI;


import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.HGAtomAttrib;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGSubsumes;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.TxCacheMap;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.PredefinedTypesConfig;

public class JavaTypeSchema implements HGTypeSchema<Class<?>>
{
    private static final int MAX_CLASS_TO_TYPE = 2000;    
    private String predefinedTypes = "/org/hypergraphdb/types";
    private HyperGraph graph;
    private JavaTypeMapper javaTypes = new JavaTypeFactory(); // new DefaultJavaTypeMapper();
    
    // Associates Java class and their corresponding HGDB types. Classes can be 
    // seen as identifiers for HGDB Java types and this cache is populated
    // on a need by need basis by looking up the type system URI->HGHandle
    // database. But the purpose of this is not only caching since different
    // class loaders may yield different versions of the same class (as identified
    // by its fully qualified name).
    private TxCacheMap<Class<?>, HGHandle> classToAtomType = null; //new ClassToTypeCache();

    // per transaction map using during type construction to avoid
    // circularity in recursive types
    private static final Map<Class<?>, HGHandle> emptyMap = new HashMap<Class<?>, HGHandle>();
    Map<Class<?>, HGHandle> getLocalIdMap()
    {
        HGTransaction tx = graph.getTransactionManager().getContext().getCurrent();
        if (tx == null)
            return emptyMap;
        Map<Class<?>, HGHandle> m = tx.getAttribute(JavaTypeSchema.class.getName() + ".idmap");
        if (m == null)
        {
            m = new HashMap<Class<?>, HGHandle>();
            tx.setAttribute(JavaTypeSchema.class.getName() + ".idmap", m);
        }
        return m;
    }
    
    public JavaTypeSchema()
    {        
    }
    
    public JavaTypeSchema(HyperGraph graph)
    {
        initialize(graph);
    }
    
    public JavaTypeMapper getJavaTypeFactory() { return javaTypes; }
    
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
    
    public String getName()
    {
        return "javaclass";
    }

    public boolean isPresent(HyperGraph graph)
    {
        return graph.getTypeSystem().getHandleForIdentifier(this.toTypeURI(HGPlainLink.class)) != null;
    }
    
    public void initialize(HyperGraph graph)
    {
        this.graph = graph;
        this.classToAtomType = new TxCacheMap<Class<?>, HGHandle>(
                graph.getTransactionManager(), ClassToTypeCache.class, this);        
        classToAtomType.load(Top.class, graph.getHandleFactory().topTypeHandle()); // TOP is its own type
        classToAtomType.load(Object.class, graph.getHandleFactory().topTypeHandle()); // TOP also corresponds to the java.lang.Object "top type"
        classToAtomType.load(HGLink.class, graph.getHandleFactory().linkTypeHandle());
        classToAtomType.load(HGSubsumes.class, graph.getHandleFactory().subsumesTypeHandle());        
        if (!isPresent(graph))
        {
            PredefinedTypesConfig config = PredefinedTypesConfig.loadFromResource(graph.getHandleFactory(), 
                                                                                  this.predefinedTypes);            
            for (HGPersistentHandle typeHandle : config.getHandles())
            {
                Class<? extends HGAtomType> cl = config.getTypeImplementation(typeHandle);
                if (cl.equals(Top.class) || 
                    cl.equals(LinkType.class) ||  
                    cl.equals(SubsumesType.class) ||
                    cl.equals(NullType.class))
                    continue;
                HGAtomType typeInstance = null;
                try { typeInstance = cl.newInstance(); }
                catch (Exception ex) { System.err.println("[HYPERGRAPHDB WARNING]: failed to create instance of type '" + 
                            cl.getName() + "'"); ex.printStackTrace(System.err); }
                List<Class<?>> targets = config.getMappedClasses(typeHandle);
                if (targets.isEmpty())
                    graph.getTypeSystem().addPredefinedType(typeHandle, typeInstance, (URI)null);
                else for (Class<?> target : targets)
                    graph.getTypeSystem().addPredefinedType(typeHandle, typeInstance, target);                
            }
        }
        javaTypes.setHyperGraph(graph);               
        
        // TODO : this is being initialized here because it causes a rather weird issue having to do
        // with the MVCC implementation if initialized on the spot (the first time it is needed). It 
        // causes the HGPlainLink.class HGDB type to have a null link pointing to it, presumably 
        // HGSubsumes that ends up being null (not in the store). There's some self-referentiality 
        // involved since AtomProjection is a bean, a RecordType and the RecordType implementation
        // does rely on the AtomProjection type being already defined. But before the MVCC this used
        // to work without a problem. Anyway, putting the initialization here fixed it, but it might
        // be just a workaround to a deeper problem that we haven't gotten to the bottom of. --Boris
        graph.getTypeSystem().getTypeHandle(AtomProjection.class);        
    }
    
    public HGAtomType defineType(URI typeId, HGHandle typeHandle, Class<?> descriptor)
    {
        return null;
    }

    public Class<?> getTypeDescriptor(URI typeId)
    {
        try
        {
            Class<?> cl = HGUtils.loadClass(graph, uriToClassName(typeId));
            if (cl.isPrimitive())
                cl = BonesOfBeans.wrapperEquivalentOf(cl);
            return cl;
        }
        catch (ClassNotFoundException e)
        {
            throw new HGException(e);
        }
    }

    public HGAtomType toRuntimeType(HGHandle typeHandle, HGAtomType typeInstance)
    {
        HGAtomType result = typeInstance;
        Set<URI> uris = graph.getTypeSystem().getIdentifiersForHandle(typeHandle); //getClassToTypeDB().findFirstByValue(typeHandle);
        for (URI u : uris)
        {
            if (!u.getScheme().equals(this.getName()))
                continue;
            String classname = uriToClassName(u);
            if (classname != null)
            {         
                Class<?> clazz;
                try
                {
                    clazz = HGUtils.loadClass(graph, classname);
                }
                catch (ClassNotFoundException e)
                {
                    // TODO we should have an option that doesn't throw an exception, but just
                    // returns the current type, without wrapping it?
                    throw new HGException(e);
                }
                result = javaTypes.getJavaBinding(typeHandle, typeInstance, clazz);
                classToAtomType.load(clazz, typeHandle);
            }
        }
        return result;
    }

    public HGAtomType fromRuntimeType(HGHandle typeHandle, HGAtomType typeInstance)
    {
    	if (typeInstance instanceof JavaBeanBinding)
    		return ((JavaBeanBinding)typeInstance).getHGType();
    	else
    		return typeInstance;
    }
    
    public URI toTypeURI(Object object)
    {
        return object == null ? null : toTypeURI(object.getClass());
    }

    public HGHandle findType(URI typeId)
    {
        return findType(getTypeDescriptor(typeId));
    }

    public HGHandle findType(Class<?> clazz)
    {
        Map<Class<?>, HGHandle> m = getLocalIdMap();        
        HGHandle typeHandle = m.get(clazz);
        if (typeHandle == null)
            typeHandle = classToAtomType.get(clazz);
        if (typeHandle == null)
            typeHandle = graph.getTypeSystem().getHandleForIdentifier(classNameToURI(clazz.getName()));
        return typeHandle;    	
    }
    
    public void removeType(URI typeId)
    {
        // We may have different version of the class being loaded by
        // different class loaders. We need to make sure all version are
        // removed from the cache since the type is no longer in HGDB.
        Set<Class<?>> S = new HashSet<Class<?>>();
        String clname = uriToClassName(typeId);
        for (Class<?> cl : classToAtomType.keySet())
            if (cl.getName().equals(clname))
                S.add(cl);
        for (Class<?> cl : S)
            classToAtomType.remove(cl);
    }
    
    public URI toTypeURI(Class<?> javaClass)
    {
        return classNameToURI(javaClass.getName());
    }

    public static String uriToClassName(URI uri)
    {
        return uri.getRawSchemeSpecificPart();
    }
    
    public static URI classNameToURI(String classname)
    {
        try
        {
            return new URI("javaclass:" + classname);
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public static URI classToURI(Class<?> cl)
    {
        return classNameToURI(cl.getName());
    }

    public void defineType(URI typeId, HGHandle typeHandle)
    {
        Class<?> clazz = this.getTypeDescriptor(typeId);
        
        Map<Class<?>, HGHandle> m = getLocalIdMap();
        m.put(clazz, typeHandle);
        //
        // First, create a dummy type for the class, so that recursive type
        // references don't lead to an infinite recursion here.
        //
        graph.define(typeHandle, graph.getHandleFactory().nullTypeHandle(), new Object(), 0);        
        
        HGHandle inferred = defineNewJavaTypeTransaction(typeHandle, clazz);
        if (inferred == null)
        {
            m.remove(clazz);
            graph.remove(typeHandle);
            throw new NoHyperGraphTypeException(typeId);
        }
        else
        {
            classToAtomType.put(clazz, inferred);           
        }        
    }
    
    /**
     * We need to infer to HG type by introspection. We maintain the
     * full inheritance tree of Java class and interfaces. Therefore, for each
     * newly added Java type mapping, we navigate to parent classes etc.
     */
    HGHandle defineNewJavaTypeTransaction(HGHandle newHandle, Class<?> clazz)
    {
        //
        // Create a HyperGraph type matching the Java class.
        //
        HGAtomType inferredHGType = javaTypes.defineHGType(clazz, newHandle);

        if (inferredHGType == null)
            return null;
            // throw new HGException("Could not create HyperGraph type for class '" + clazz.getName() + "'");

        //
        // Now, replace the dummy atom that we added at the beginning with the new type.
        //
        HGHandle typeConstructor = graph.getTypeSystem().getTypeHandle(inferredHGType.getClass());
        if (!typeConstructor.equals(graph.getTypeSystem().getTop()))
            graph.replace(newHandle, inferredHGType, typeConstructor);
        //
        // TODO: we are assuming here that if the defineHGType call above did not return
        // a newly created type, but an already existing one, its type constructor can
        // only be Top. There is no reason this should be true in general. Probably
        // defineHGType should return multiple values: the resulting type, whether it's new
        // or not and possibly other things, encapsulated in some sort of type descriptor.
        //
        else
        {
            throw new HGException("Type constructor of newly defined type is TOP, what gives?");
            /*
            // We have a predefined type. We must erase the dummy atom instead of replacing it.
            graph.remove(newHandle);

            // A predefined type is a frozen atom in the cache, so its live handle is available
            // from the Java instance.
            HGHandle result = graph.getCache().get(inferredHGType);

            //
            // Update the Class -> type handle mapping.
            //
            classToAtomType.put(clazz, result);
            getClassToTypeDB().removeEntry(clazz.getName(), graph.getPersistentHandle(newHandle));
            getClassToTypeDB().addEntry(clazz.getName(), graph.getPersistentHandle(result));
            newHandle = result;
            
            */
        }

        //
        // So far, the inferredHGType is platform independent HyperGraph type, e.g.
        // a RecordType. To make it transparently handle run-time instances of 'clazz',
        // we need a corresponding Java binding for this type, e.g. a JavaBeanBinding.
        //
        HGAtomType type = javaTypes.getJavaBinding(newHandle, inferredHGType, clazz);
        type.setHyperGraph(graph);

        //
        // the result of hg.add may or may not be stored in the cache: if it is, replace the run-time
        // instance of
        //
        if (! (newHandle instanceof HGLiveHandle) )
            // First get the corresponding live handle
            newHandle = graph.getCache().atomRead((HGPersistentHandle)newHandle,
                                                  type, 
                                                  new HGAtomAttrib());
            
        newHandle = graph.getCache().atomRefresh((HGLiveHandle)newHandle, type, true);

        // Now, examine the super type and implemented interfaces
        // First, make sure we've mapped all interfaces
        Class<?> [] interfaces = clazz.getInterfaces();
        for (int i = 0; i < interfaces.length; i++)
        {
            HGHandle interfaceHandle = graph.getTypeSystem().getTypeHandle(interfaces[i]);
            if (interfaceHandle == null)
                throw new HGException("Unable to infer HG type for interface " +
                                       interfaces[i].getName());
            else
                graph.getTypeSystem().assertSubtype(interfaceHandle, newHandle);
        }
        //
        // Next, navigate to the superclass.
        //
        if (clazz.getSuperclass() != null)
        {
            HGHandle superHandle = graph.getTypeSystem().getTypeHandle(clazz.getSuperclass());
            // Then proceed recursively to the superclass:
            if (superHandle == null)
            {
                throw new HGException("Unable to infer HG type for class " +
                                      clazz.getSuperclass().getName() +
                                      " the superclass of " + clazz.getName());
            }
            else
                graph.getTypeSystem().assertSubtype(superHandle, newHandle);
        }
        // Interfaces don't derive from java.lang.Object, so we need to super-type them with Top explicitly
        else if (clazz.isInterface())
            graph.add(new HGSubsumes(graph.getTypeSystem().getTop(), newHandle));

        //
        // ouf, we're done
        //
        return newHandle;
    }
    
    public class ClassToTypeCache extends LinkedHashMap<Class<?>, HGHandle>
    {
        static final long serialVersionUID = -1;

        @SuppressWarnings("unused")
        public ClassToTypeCache()
        {
            super(1000, 0.75f, true);
        }
        
        protected boolean removeEldestEntry(Map.Entry<Class<?>, HGHandle> eldest)
        {
            if (size() > MAX_CLASS_TO_TYPE)
            {
                if (eldest.getValue() instanceof HGLiveHandle)
                {
                    HGLiveHandle h = (HGLiveHandle)eldest.getValue();
                    if (h.getRef() == null)
                        return true; //if it has been evicted from the atom cache, removed it from here too
                    else if (graph.getCache().isFrozen(h))
                        return get(eldest.getKey()) == null; // this will return false and put the element on top of the list
                    else
                        return false; // simply return false, but don't remove since it's still in the cache
                }
                else
                {
                    HGLiveHandle h = graph.getCache().get((HGPersistentHandle)eldest.getValue());
                    if (h != null)
                    {
                        eldest.setValue(h);
                        return false;
                    }
                    else
                        return true;
                }
            }
            else
                return false;
        }
    }    
}
