package hgdbteststorage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import org.junit.Assert;
import hgtest.T;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.junit.Test;

/**
 * Tests for the top-level storage methods such as storing atoms and their incidence sets.
 * 
 * @author borislav
 *
 */
public class TopLevelStorageTests extends StoreImplementationTestBase
{
	@Test
	public void rawData()
	{
		int valuesCount = T.random(50, 200);
		HashMap<HGHandle, byte[]> data = new HashMap<HGHandle, byte[]>();  
		for (int i = 0; i < valuesCount; i++)
		{
			byte [] value  = T.randomBytes(T.random(10000));
			HGHandle handle = config().getHandleFactory().makeHandle();
			data.put(handle, value);
			impl().store(handle.getPersistent(), value);
		}
		data.forEach( (key, value) -> {
			Assert.assertFalse(impl().containsLink(key.getPersistent()));
			Assert.assertTrue(impl().containsData(key.getPersistent()));
		});		
		data.forEach( (key, value) -> {
			Assert.assertArrayEquals(value, impl().getData(key.getPersistent()));
			impl().removeData(key.getPersistent());
			Assert.assertNull(impl().getData(key.getPersistent()));
		});
	}
	
	@Test
	public void testLinks()
	{
		int recordCount = T.random(50, 200);
		HashMap<HGPersistentHandle, HGPersistentHandle[]> data = new HashMap<HGPersistentHandle, HGPersistentHandle[]>();  
		for (int i = 0; i < recordCount; i++)
		{
			HGPersistentHandle [] links = new HGPersistentHandle[T.random(0, 10000)];
			for (int j = 0; j < links.length; j++)
				links[j] = config().getHandleFactory().makeHandle();
			HGPersistentHandle handle = config().getHandleFactory().makeHandle();
			impl().store(handle, links);
			data.put(handle, links);
		}
		data.forEach( (key, value) -> {
			Assert.assertFalse(impl().containsData(key.getPersistent()));
			Assert.assertTrue(impl().containsLink(key.getPersistent()));
		});		
		data.forEach( (key, value) -> {
			Assert.assertArrayEquals(value, impl().getLink(key.getPersistent()));
			impl().removeData(key.getPersistent());
			Assert.assertNull(impl().getData(key.getPersistent()));
		});
	}
	
	@Test
	public void testEmptyIncidenceSet()
	{
		HGPersistentHandle handle = config().getHandleFactory().makeHandle();
		Assert.assertEquals(0, impl().getIncidenceSetCardinality(handle));
		HGRandomAccessResult<HGPersistentHandle> rs = impl().getIncidenceResultSet(handle);
		Assert.assertFalse(rs.hasNext());
		Assert.assertFalse(rs.hasPrev());
		Assert.assertEquals(GotoResult.nothing, rs.goTo(config().getHandleFactory().makeHandle(), true));
		Assert.assertEquals(GotoResult.nothing, rs.goTo(config().getHandleFactory().makeHandle(), false));
		rs.goAfterLast();
		rs.goBeforeFirst();
		try { rs.current(); } catch (Exception ex) { Assert.assertTrue(ex instanceof NoSuchElementException); }
		try { rs.next(); } catch (Exception ex) { Assert.assertTrue(ex instanceof NoSuchElementException); }
		try { rs.prev(); } catch (Exception ex) { Assert.assertTrue(ex instanceof NoSuchElementException); }		
		rs.close();
	}
	
	@Test(expected=NullPointerException.class) 
	public void testAddNullIncident()
	{
		impl().addIncidenceLink(hfactory().makeHandle(), null);
	}
	
	@Test(expected=NullPointerException.class) 
	public void testAddIncidentToNull()
	{
		impl().addIncidenceLink(null, hfactory().makeHandle());
	}

	@Test(expected=NullPointerException.class) 
	public void testAddNullData()
	{
		impl().store(hfactory().makeHandle(), (byte[])null);
	}

	@Test 
	public void testAddEmptyData()
	{
		HGPersistentHandle h = hfactory().makeHandle(); 
		impl().store(h, new byte[0]);
		Assert.assertEquals(0, impl().getData(h).length);
	}
	
	@Test(expected=NullPointerException.class) 
	public void testAddDatatoNull()
	{
		impl().store(null, new byte[]{2,3,4});
	}

	@Test(expected=NullPointerException.class) 
	public void testAddNullLink()
	{
		impl().store(hfactory().makeHandle(), (HGPersistentHandle[])null);
	}

	@Test 
	public void testAddEmptyLink()
	{
		HGPersistentHandle h = hfactory().makeHandle(); 
		impl().store(h, new HGPersistentHandle[0]);
		Assert.assertEquals(0, impl().getLink(h).length);
	}
	
	@Test(expected=NullPointerException.class) 
	public void testAddLinkToNull()
	{
		impl().store(null, new HGPersistentHandle[]{hfactory().makeHandle()});
	}
	
	@Test 
	public void testIncidenceSets()
	{		
		int recordCount = T.random(50, 200);
		HashMap<HGPersistentHandle, TreeSet<HGPersistentHandle>> data = new HashMap<HGPersistentHandle, TreeSet<HGPersistentHandle>>();  
		for (int i = 0; i < recordCount; i++)
		{
			HGPersistentHandle handle = config().getHandleFactory().makeHandle();			
			int incidentCount = T.random(0, 10000);
			TreeSet<HGPersistentHandle> IS = new TreeSet<HGPersistentHandle>();
			for (int j = 0; j < incidentCount; j++)
			{
				HGPersistentHandle incident = config().getHandleFactory().makeHandle();
				IS.add(incident);
				impl().addIncidenceLink(handle, incident);
			}
		}
		data.forEach( (key, value) -> {
			Assert.assertEquals(data.get(key).size(), impl().getIncidenceSetCardinality(key));
		});		
		
		// Test a few incident link removals
		data.forEach( (key,value) -> {
			int pos = T.random(0, value.size());
			HGPersistentHandle incident = null;
			if (pos > 0 && T.random(0, 2) == 0)
			{
				Iterator<HGPersistentHandle> iter = value.iterator();
				while (pos-- > 0)
					incident = iter.next();
				value.remove(incident);
				impl().removeIncidenceLink(key, incident);
			}
			HGRandomAccessResult<HGPersistentHandle> rs = impl().getIncidenceResultSet(key);
			if (incident != null)
				Assert.assertEquals(GotoResult.nothing, rs.goTo(incident, true));
			else
				Assert.assertEquals(GotoResult.found, rs.goTo(incident, true));
			rs.close();
		});
		
		data.forEach( (key, value) -> {
			HGRandomAccessResult<HGPersistentHandle> rs = impl().getIncidenceResultSet(key);
			Iterator<HGPersistentHandle> ts = value.iterator();
			HGPersistentHandle last = null;
			while (ts.hasNext())
			{
				Assert.assertTrue(rs.hasNext());				
				Assert.assertEquals(ts.next(), rs.next());
				last = rs.current();
			}			
			rs.goBeforeFirst();
			if (last != null) // non-empty
			{
				Assert.assertEquals(value.iterator().next(), rs.next());
				rs.goAfterLast();
				Assert.assertEquals(value.iterator().next(), rs.prev());
			}
			rs.close();
			impl().removeIncidenceSet(key);
			Assert.assertNull(impl().getData(key.getPersistent()));
		});
	}
	
	@Test
	public void testNonExistingIndex()
	{
		Assert.assertNull(impl().getIndex("askdfjaksjhfalksjdfhurhkjsgflsjdfglsdjfhglsdufghu9q3urhggsdfg"));
	}
}