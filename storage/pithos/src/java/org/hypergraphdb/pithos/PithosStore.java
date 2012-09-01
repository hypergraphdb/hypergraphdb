package org.hypergraphdb.pithos;

import java.util.Comparator;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.transaction.HGTransactionFactory;

public class PithosStore implements HGStoreImplementation
{
	private PithosConfig config = new PithosConfig();
	
	public Object getConfiguration()
	{
	    return config;
	}

	public void startup(HGStore store, HGConfiguration configuration)
	{
	}

	public void shutdown()
	{
	}

	public HGTransactionFactory getTransactionFactory()
	{
		return null;
	}

	public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
	{
		return null;
	}

	public HGPersistentHandle[] getLink(HGPersistentHandle handle)
	{
		return null;
	}

	public void removeLink(HGPersistentHandle handle)
	{
	}

	public boolean containsLink(HGPersistentHandle handle)
	{
		return false;
	}

	public boolean containsData(HGPersistentHandle handle)
	{
		return false;
	}
	
	public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
	{
		return null;
	}

	public void removeData(HGPersistentHandle handle)
	{
	}

	public byte[] getData(HGPersistentHandle handle)
	{
		return null;
	}

	public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(
			HGPersistentHandle handle)
	{
		return null;
	}

	public void removeIncidenceSet(HGPersistentHandle handle)
	{
	}

	public long getIncidenceSetCardinality(HGPersistentHandle handle)
	{
		return 0;
	}

	public void addIncidenceLink(HGPersistentHandle handle,
			HGPersistentHandle newLink)
	{
		// TODO Auto-generated method stub

	}

	public void removeIncidenceLink(HGPersistentHandle handle,
			HGPersistentHandle oldLink)
	{
		// TODO Auto-generated method stub

	}

	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
			String name, ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter,
			Comparator<?> comparator, boolean isBidirectional,
			boolean createIfNecessary)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public void removeIndex(String name)
	{
		// TODO Auto-generated method stub

	}
}