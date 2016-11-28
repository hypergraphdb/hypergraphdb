package hgtest.storage.bje.LinkBinding;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.junit.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LinkBinding_readHandlesTest extends LinkBindingTestBasis
{
	@Test
	public void throwsException_whenBufferIsNull() throws Exception
	{
		below.expect(NullPointerException.class);
		binding.readHandles(null, 0, 100);
	}

	@Test
	public void throwsException_whenOffsetIsNegative() throws Exception
	{
		final byte[] buffer = new byte[] { 0, 0, 0, 0 };

		below.expect(ArrayIndexOutOfBoundsException.class);
		below.expectMessage("-1");
		binding.readHandles(buffer, -1, 4);
	}

	@Test
	public void returnsEmptyArrayOfHandles_whenLengthIsNegative()
			throws Exception
	{
		final byte[] buffer = new byte[] { 0, 0, 0, 0 };

		final HGPersistentHandle[] result = binding.readHandles(buffer, 0, -1);

		assertThat(result.length, is(0));
	}

	@Test
	public void returnsOneIntegerHandle_whenThereAreFourBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new IntPersistentHandle(
				0) };

		final byte[] buffer = new IntPersistentHandle(0).toByteArray();

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 4);

		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsTwoIntegerHandles_whenThereAreEightBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2) };

		final byte[] buffer = intHandlesAsByteArray(1, 2);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 8);

		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsThreeIntegerHandles_whenThereAreTwelveBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2),
				new IntPersistentHandle(3) };

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 12);

		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsEmptyArrayOfHandles_whenBufferIsEmptyr()
			throws Exception
	{
		final HGPersistentHandle[] result = binding.readHandles(new byte[] {},
				0, 0);

		assertThat(result.length, is(0));
	}

	@Test
	public void returnsTwoIntegerHandles_whenBytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreEnoughBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2) };

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 10);

		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsThreeIntegerHandles_whenBytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreInsufficientBytesInBuffer()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(1), new IntPersistentHandle(2),
				new IntPersistentHandle(3) };

		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);

		final HGPersistentHandle[] actual = binding.readHandles(buffer, 0, 15);

		assertArrayEquals(expected, actual);
	}
}
