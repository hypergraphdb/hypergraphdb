package org.hypergraphdb.storage;

import java.util.Comparator;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.transaction.HGTransactionFactory;

/**
 * <p>
 * TODO - this could be used as a wrapper for any storage implementation, a wrapper
 * that allows incidence set indexing to be augmented with additional attributes. 
 * </p>
 * 
 * @author borislav
 *
 */
public class StorageWithAnnotatedIncidence implements HGStoreImplementation
{
    private HGStoreImplementation wrapped;
    
    public StorageWithAnnotatedIncidence(HGStoreImplementation wrapped)
    {
        this.wrapped = wrapped;
    }
    
    public Object getConfiguration()
    {
        return wrapped.getConfiguration();
    }

    public void startup(HGStore store, HGConfiguration configuration)
    {
        wrapped.startup(store, configuration);
    }

    public void shutdown()
    {
        wrapped.shutdown();
    }

    public HGTransactionFactory getTransactionFactory()
    {
        return wrapped.getTransactionFactory();
    }

    public HGPersistentHandle store(HGPersistentHandle handle,
                                    HGPersistentHandle[] link)
    {
        return wrapped.store(handle, link);
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        return wrapped.getLink(handle);
    }

    public void removeLink(HGPersistentHandle handle)
    {
        wrapped.removeLink(handle);
    }

    public boolean containsLink(HGPersistentHandle handle)
    {
        return wrapped.containsLink(handle);
    }

    public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
    {
        return wrapped.store(handle, data);
    }

    public byte[] getData(HGPersistentHandle handle)
    {
        return wrapped.getData(handle);
    }

    public void removeData(HGPersistentHandle handle)
    {
        wrapped.removeData(handle);
    }

    public boolean containsData(HGPersistentHandle handle)
    {
        return wrapped.containsData(handle);
    }

    public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle)
    {
        return wrapped.getIncidenceResultSet(handle);
    }

    public void removeIncidenceSet(HGPersistentHandle handle)
    {
        wrapped.removeIncidenceSet(handle);
    }

    public long getIncidenceSetCardinality(HGPersistentHandle handle)
    {
        return wrapped.getIncidenceSetCardinality(handle);
    }

    public void addIncidenceLink(HGPersistentHandle handle,
                                 HGPersistentHandle newLink)
    {
        wrapped.addIncidenceLink(handle, newLink);
    }

    public void removeIncidenceLink(HGPersistentHandle handle,
                                    HGPersistentHandle oldLink)
    {
        wrapped.removeIncidenceLink(handle, oldLink);
    }


	@Override
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name)
	{
		return wrapped.getIndex(name);
	}   
	
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name,
                                                                     ByteArrayConverter<KeyType> keyConverter,
                                                                     ByteArrayConverter<ValueType> valueConverter,
                                                                     Comparator<?> comparator,
                                                                     boolean isBidirectional,
                                                                     boolean createIfNecessary)
    {
        return wrapped.getIndex(name, keyConverter, valueConverter, comparator,
                isBidirectional, createIfNecessary);
    }

    public void removeIndex(String name)
    {
        wrapped.removeIndex(name);
    } 
}