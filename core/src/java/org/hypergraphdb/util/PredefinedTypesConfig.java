package org.hypergraphdb.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.type.HGAtomType;

/**
 * 
 * <p>
 * Utility to read and hold configuration of predefined HyperGraphDB types.
 * A predefined types configuration file is a text file with the following format:
 * <ul>
 * <li>Each line contains a single predefined type as a list of space-separated
 * fields.</li>
 * <li>
 * <li>
 * The first field is the string representation of the persistent handle of the type. 
 * </li>
 * <li>
 * The second field is the Java class implementing the type. It must be a default
 * constructible implementation of the {@link HGAtomType} interface.
 * </li>
 * <li>
 * Finally, zero or more fields list every Java class that this type maps to. 
 * </li>
 * <li>
 * Lines starting with the pound sign # are ignored.
 * </li>
 * </ul>
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class PredefinedTypesConfig
{    
    private static ArrayList<String> readIt(InputStream in)
    {
        try
        {
            ArrayList<String> L = new ArrayList<String>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));            
            for (String line = reader.readLine(); line != null; line = reader.readLine())
                if (line.trim().length() > 0 && !line.startsWith("#")) 
                    L.add(line.trim());
            return L;
        }
        catch (IOException ex)
        {
            throw new HGException(ex);
        }
    }
    
    private TwoWayMap<HGPersistentHandle, Class<? extends HGAtomType>>
      handleToImpl = new TwoWayMap<HGPersistentHandle, Class<? extends HGAtomType>>();
    private HashMap<HGPersistentHandle, List<Class<?>>> 
      handleToTargets = new HashMap<HGPersistentHandle, List<Class<?>>>();
    
    @SuppressWarnings("unchecked")
    private void loadTypes(HGHandleFactory handleFactory, ArrayList<String> L)
    {
        for (String line : L)
        {
            String [] A = line.split("\\s+");
            HGPersistentHandle handle = handleFactory.makeHandle(A[0]);
            String typeClassName = A[1];
            Class<? extends HGAtomType> typeClass = null; 
            try
            {
                typeClass = (Class<? extends HGAtomType>) Class.forName(typeClassName);
                handleToImpl.add(handle, typeClass);
            }
            catch (Exception ex)
            {
                System.err.println("[HYPERGRAPHDB WARNING]: unable to load type class '" + 
                        typeClassName + "'");
            }
            ArrayList<Class<?>> targets = new ArrayList<Class<?>>();
            handleToTargets.put(handle, targets);
            for (int i = 2; i < A.length; i++)
            {
                Class<?> cl = null; 
                try
                {
                    cl = Class.forName(A[i]);
                    targets.add(cl);
                }
                catch (Exception ex)
                {
                    System.err.println("[HYPERGRAPHDB WARNING]: unable to load class '" + 
                            A[i] + "' for HG type " + "'" + typeClassName + "'");
                }
            }
        }
    }
    
    private PredefinedTypesConfig()
    {
        
    }
    
    public Collection<HGPersistentHandle> getHandles()
    {
        return handleToImpl.getXSet();
    }
    
    public HGPersistentHandle getHandleOf(Class<? extends HGAtomType> typeImplementation)
    {
        return handleToImpl.getX(typeImplementation);
    }
    
    public Class<? extends HGAtomType> getTypeImplementation(HGPersistentHandle typeHandle)
    {
        return handleToImpl.getY(typeHandle);
    }

    public List<Class<?>> getMappedClasses(HGPersistentHandle typeHandle)
    {
        return handleToTargets.get(typeHandle);
    }
    
    public static PredefinedTypesConfig loadFromResource(HGHandleFactory handleFactory, String resource)
    {
        InputStream in = PredefinedTypesConfig.class.getResourceAsStream(resource);
        try
        {
            PredefinedTypesConfig config = new PredefinedTypesConfig();
            config.loadTypes(handleFactory, readIt(in));
            return config;
        }
        catch (Exception ex)
        {
            throw new HGException(ex);
        }        
        finally
        {
            try {in.close();}catch (Throwable t) {}
        }
    }
    
    public static PredefinedTypesConfig loadFromFile(HGHandleFactory handleFactory, File file)
    {
        InputStream in = null;
        try
        {
            in = new FileInputStream(file);
            PredefinedTypesConfig config = new PredefinedTypesConfig();
            config.loadTypes(handleFactory, readIt(in));
            return config;
        }
        catch (Exception ex)
        {
            throw new HGException(ex);
        }        
        finally
        {
            try {if (in != null) in.close();}catch (Throwable t) {}
        }        
    }
}
