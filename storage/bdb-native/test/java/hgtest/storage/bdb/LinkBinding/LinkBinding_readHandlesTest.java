package hgtest.storage.bdb.LinkBinding;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class LinkBinding_readHandlesTest extends LinkBindingTestBasis
{
@Test
	public void bufferIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		try
		{
			binding.readHandles(null, 0, 100);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void offsetIsNegative() throws Exception
	{
		final Exception expected = new ArrayIndexOutOfBoundsException("-1");

		final byte[] buffer = new byte[] { 0, 0, 0, 0 };

		try
		{
			binding.readHandles(buffer, -1, 4);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void lengthIsNegative() throws Exception
	{
		final byte[] buffer = new byte[] { 0, 0, 0, 0 };

		final HGPersistentHandle[] result = binding.readHandles(buffer, 0, -1);

		assertEquals(result.length, 0);
	}

	@Test
	public void thereAreFourBytesInBuffer() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new IntPersistentHandle(
				0) };

		final byte[] buffer = new IntPersistentHandle(0).toByteArray();

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 4);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreEightBytesInBuffer() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2) };

		final byte[] buffer = intHandlesAsByteArray(1, 2);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 8);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreTwelveBytesInBuffer() throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2),
				new IntPersistentHandle(3) };

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 12);

		assertEquals(actual, expected);
	}

	@Test
	public void thereAreZeroBytesInBuffer() throws Exception
	{
		final HGPersistentHandle[] result = binding.readHandles(new byte[] {},
				0, 0);

		assertEquals(result.length, 0);
	}

	@Test
	public void BytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreEnoughBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2) };

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 10);

		assertEquals(actual, expected);
	}

	@Test
	public void BytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreInsufficientBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2),
				new IntPersistentHandle(3) };

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 15);

		assertEquals(actual, expected);
	}
}
