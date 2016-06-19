package hgtest.storage.bje.LinkBinding;

import com.sleepycat.bind.tuple.TupleInput;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.junit.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static java.lang.System.arraycopy;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LinkBinding_entryToObjectTest extends LinkBindingTestBasis
{
	@Test
	public void throwsException_whenInputIsNull() throws Exception
	{
		below.expect(NullPointerException.class);
		binding.entryToObject((TupleInput) null);
	}

	@Test
	public void returnsEmptyArrayOfHandles_whenInputArrayIsEmpty()
			throws Exception
	{
		final TupleInput input = new TupleInput(new byte[] {});

		final HGPersistentHandle[] result = binding.entryToObject(input);

		assertEquals(result.length, 0);
	}

	@Test
	public void returnsOneHandle_whenThereAreFourBytesInInput()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new IntPersistentHandle(
				1) };

		final TupleInput input = new TupleInput(intHandlesAsByteArray(1));

		final HGPersistentHandle[] actual = binding.entryToObject(input);

        assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsTwoHandles_whenThereAreEightBytesInInput()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(2), new IntPersistentHandle(5) };

		final TupleInput input = new TupleInput(intHandlesAsByteArray(2, 5));

		final HGPersistentHandle[] actual = binding.entryToObject(input);

		assertArrayEquals(expected, actual);
	}

	@Test
	public void returnsThreeHandles_whenThereAreTwelveBytesInInput()
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new IntPersistentHandle(2), new IntPersistentHandle(5),
				new IntPersistentHandle(10) };

		final TupleInput input = new TupleInput(intHandlesAsByteArray(2, 5, 10));

		final HGPersistentHandle[] actual = binding.entryToObject(input);

		assertArrayEquals(expected, actual);
	}

	@Test
	public void throwsException_whenBytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreEnoughBytesInTheBuffer()
			throws Exception
	{
		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);
		final byte[] truncatedBuffer = new byte[10];
		arraycopy(buffer, 0, truncatedBuffer, 0, 10);
		final TupleInput input = new TupleInput(truncatedBuffer);

		below.expect(HGException.class);
		below.expectMessage("While reading link tuple: the value buffer size is not a multiple of the handle size.");
		binding.entryToObject(input);
	}

	@Test
	public void throwsException_whenBytesCount_Div_HandleSize_IsNotEqualToZero_AndThereAreInsufficientBytesInTheBuffer()
			throws Exception
	{
		final byte[] buffer = intHandlesAsByteArray(1, 2, 3);
		final byte[] truncatedBuffer = new byte[15];
		arraycopy(buffer, 0, truncatedBuffer, 0, 10);
		final TupleInput input = new TupleInput(truncatedBuffer);

		below.expect(HGException.class);
		below.expectMessage("While reading link tuple: the value buffer size is not a multiple of the handle size.");
		binding.entryToObject(input);
	}
}
