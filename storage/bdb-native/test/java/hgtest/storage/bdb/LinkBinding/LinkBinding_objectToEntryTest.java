package hgtest.storage.bdb.LinkBinding;

import com.sleepycat.bind.tuple.TupleOutput;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class LinkBinding_objectToEntryTest extends LinkBindingTestBasis
{
	@Test
	public void linkArrayIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final HGPersistentHandle[] link = null;
		final TupleOutput output = new TupleOutput(new byte[4]);

		try
		{
			binding.objectToEntry(link, output);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void outputIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final HGPersistentHandle[] link = new HGPersistentHandle[] { new IntPersistentHandle(
				1) };
		final TupleOutput output = null;

		try
		{
			binding.objectToEntry(link, output);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void linkArrayIsEmpty() throws Exception
	{
		final HGPersistentHandle[] link = new HGPersistentHandle[] {};
		final TupleOutput output = new TupleOutput(new byte[] {});

		binding.objectToEntry(link, output);

		assertEquals(output.getBufferBytes(), new byte[] {});
	}

	@Test
	public void thereIsOneHandle() throws Exception
	{
		final byte[] expected = intHandlesAsByteArray(1);

		final HGPersistentHandle[] link = new HGPersistentHandle[] { new IntPersistentHandle(
				1) };
		final TupleOutput output = new TupleOutput(new byte[4]);

		binding.objectToEntry(link, output);

		assertEquals(output.getBufferBytes(), expected);
	}

	@Test
	public void thereAreTwoHandles() throws Exception
	{
		final byte[] expected = intHandlesAsByteArray(5, 10);

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10) };
		final TupleOutput output = new TupleOutput(new byte[8]);

		binding.objectToEntry(link, output);

		assertEquals(output.getBufferBytes(), expected);
	}

	@Test
	public void thereAreThreeHandles() throws Exception
	{
		final byte[] expected = intHandlesAsByteArray(5, 10, 15);

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10),
				new IntPersistentHandle(15) };
		final TupleOutput output = new TupleOutput(new byte[12]);

		binding.objectToEntry(link, output);

		assertEquals(output.getBufferBytes(), expected);
	}

	@Test
	public void outputBufferIsTooShort() throws Exception
	{
		final byte[] expected = new byte[] { -128, 0, 0, 5, -128, 0, 0, 10, 0,
				0, 0, 0, 0, 0 };

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10) };
		final TupleOutput output = new TupleOutput(new byte[6]);

		binding.objectToEntry(link, output);

		assertEquals(output.getBufferBytes(), expected);
	}

	@Test
	public void outputBufferIsTooLarge() throws Exception
	{
		final byte[] expected = new byte[] { -128, 0, 0, 5, -128, 0, 0, 10, 0,
				0 };

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10) };
		final TupleOutput output = new TupleOutput(new byte[10]);

		binding.objectToEntry(link, output);

		assertEquals(output.getBufferBytes(), expected);
	}
}
