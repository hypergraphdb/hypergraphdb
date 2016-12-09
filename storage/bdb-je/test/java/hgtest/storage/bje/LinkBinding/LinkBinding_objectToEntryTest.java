package hgtest.storage.bje.LinkBinding;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.junit.Test;

import com.sleepycat.bind.tuple.TupleOutput;

public class LinkBinding_objectToEntryTest extends LinkBindingTestBasis
{
	@Test
	public void throwsException_whenArrayOfHandlesIsNull() throws Exception
	{
		final HGPersistentHandle[] link = null;
		final TupleOutput output = new TupleOutput(new byte[4]);

		below.expect(NullPointerException.class);
		binding.objectToEntry(link, output);
	}

	@Test
	public void throwsException_whenOutputTupleIsNull() throws Exception
	{
		final HGPersistentHandle[] link = new HGPersistentHandle[] { new IntPersistentHandle(
				1) };
		final TupleOutput output = null;

		below.expect(NullPointerException.class);
		binding.objectToEntry(link, output);
	}

	@Test
	public void returnsTupleWithByteBuffer_whenArrayOfHandlesIsEmpty()
			throws Exception
	{
		final HGPersistentHandle[] link = new HGPersistentHandle[] {};
		final TupleOutput output = new TupleOutput(new byte[] {});

		binding.objectToEntry(link, output);

		assertThat(output.getBufferBytes(), is(new byte[] {}));
	}

	@Test
	public void returnsTupleWithFourBytesInBuffer_whenThereIsOneIntegerHandle()
			throws Exception
	{
		final byte[] expected = intHandlesAsByteArray(1);

		final HGPersistentHandle[] link = new HGPersistentHandle[] { new IntPersistentHandle(
				1) };
		final TupleOutput output = new TupleOutput(new byte[4]);

		binding.objectToEntry(link, output);

		assertArrayEquals(expected, output.getBufferBytes());
	}

	@Test
	public void returnsTupleWithEightBytesInBuffer_whenThereAreTwoIntegerHandles()
			throws Exception
	{
		final byte[] expected = intHandlesAsByteArray(5, 10);

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10) };
		final TupleOutput output = new TupleOutput(new byte[8]);

		binding.objectToEntry(link, output);

		assertArrayEquals(expected, output.getBufferBytes());
	}

	@Test
	public void returnsTupleWithTwelveBytesInBuffer_whenThereAreThreeIntegerHandles()
			throws Exception
	{
		final byte[] expected = intHandlesAsByteArray(5, 10, 15);

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10),
				new IntPersistentHandle(15) };
		final TupleOutput output = new TupleOutput(new byte[12]);

		binding.objectToEntry(link, output);

		assertArrayEquals(expected, output.getBufferBytes());
	}

	@Test
	public void doesNotFail_whenOutputBufferIsTooShort() throws Exception
	{
		final byte[] expected = new byte[] { -128, 0, 0, 5, -128, 0, 0, 10, 0,
				0, 0, 0, 0, 0 };

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10) };
		final TupleOutput output = new TupleOutput(new byte[6]);

		binding.objectToEntry(link, output);

		assertArrayEquals(expected, output.getBufferBytes());
	}

	@Test
	public void doesNotFails_whenOutputBufferIsTooLarge() throws Exception
	{
		final byte[] expected = new byte[] { -128, 0, 0, 5, -128, 0, 0, 10, 0,
				0 };

		final HGPersistentHandle[] link = new HGPersistentHandle[] {
				new IntPersistentHandle(5), new IntPersistentHandle(10) };
		final TupleOutput output = new TupleOutput(new byte[10]);

		binding.objectToEntry(link, output);

		assertArrayEquals(expected, output.getBufferBytes());
	}
}
