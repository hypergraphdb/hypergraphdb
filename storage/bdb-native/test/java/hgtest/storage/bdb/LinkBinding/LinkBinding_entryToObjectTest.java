package hgtest.storage.bdb.LinkBinding;

import com.sleepycat.bind.tuple.TupleInput;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class LinkBinding_entryToObjectTest extends LinkBindingTestBasis
{
	@Test
	public void inputIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final TupleInput input = null;

		try
		{
			binding.entryToObject(input);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void thereAreZeroBytesInInput() throws Exception
	{
		final TupleInput input = new TupleInput(new byte[] {});

		final HGPersistentHandle[] result = binding.entryToObject(input);

		assertEquals(result.length, 0);
	}

	@Test
	public void thereAreFourBytesInInput() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new IntPersistentHandle(
				1) };

		final TupleInput input = new TupleInput(intHandlesAsByteArray(1));

		final HGPersistentHandle[] actual = binding.entryToObject(input);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreEightBytesInInput() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(2), new IntPersistentHandle(5) };

		final TupleInput input = new TupleInput(intHandlesAsByteArray(2, 5));

		final HGPersistentHandle[] actual = binding.entryToObject(input);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreTwelveBytesInInput() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(2), new IntPersistentHandle(5),
				new IntPersistentHandle(10) };

		final TupleInput input = new TupleInput(intHandlesAsByteArray(2, 5, 10));

		final HGPersistentHandle[] actual = binding.entryToObject(input);

		assertEquals(actual, expected);
	}

	@Test
	public void BytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreEnoughBytesInBuffer()
			throws Exception
	{
		final Exception expected = new HGException(
				"While reading link tuple: the value buffer size is not a multiple of the handle size.");

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);
		final byte[] truncatedBuffer = new byte[10];
		System.arraycopy(buffer, 0, truncatedBuffer, 0, 10);
		final TupleInput input = new TupleInput(truncatedBuffer);

		try
		{
			binding.entryToObject(input);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void BytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreInsufficientBytesInBuffer()
			throws Exception
	{
		final Exception expected = new HGException(
				"While reading link tuple: the value buffer size is not a multiple of the handle size.");

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);
		final byte[] truncatedBuffer = new byte[15];
		System.arraycopy(buffer, 0, truncatedBuffer, 0, 10);
		final TupleInput input = new TupleInput(truncatedBuffer);

		try
		{
			binding.entryToObject(input);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}
}
