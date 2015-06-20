package hgtest.storage.bdb;

import com.sleepycat.db.*;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.storage.ByteArrayConverter;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Some helper methods to deal with BDB specific classes.
 * 
 * @author Yuriy Sechko
 */
public class TestUtils
{
    /**
     * Creates list which contains given items.
     * <p/>
     * Items in the list appear in the same order as in parameters. More items
     * can be added to the list later.
     * <p/>
     * Usage scenario of this method:
     *
     * <pre>
     * {@code
     * List<Integer> list = list(1, 2, 3); // we have list with 3 items here }
     * </pre>
     *
     * @param items
     *            items of the list
     * @param <T>
     *            type of the items in the list
     */
    public static <T> List<T> list(final T... items)
    {
        return new ArrayList<T>(Arrays.asList(items));
    }

    /**
     * Deletes directory's content and then deletes directory itself. Deleting
     * is not recursive.
     *
     * @param directory
     */
    public static void deleteDirectory(final File directory)
    {
        final File[] filesInTestDir = directory.listFiles();
        if (filesInTestDir != null)
        {
            for (final File eachFile : filesInTestDir)
            {
                eachFile.delete();
            }
        }
        directory.delete();
    }

    /**
     * Iterates through result and copies encountered items to the list.
     */
    public static <T> List<T> list(final HGSearchResult<T> result)
    {
        final List<T> outputList = new ArrayList<T>();
        while (result.hasNext())
        {
            final T currentValue = result.next();
            outputList.add(currentValue);
        }
        return outputList;
    }

    /**
     * Iterates through result and copies encountered items to the list.
     */
    public static <T> List<T> listAndClose(final HGSearchResult<T> result)
    {
        final List<T> outputList = new ArrayList<T>();
        while (result.hasNext())
        {
            final T currentValue = result.next();
            outputList.add(currentValue);
        }
        result.close();
        return outputList;
    }

    /**
     * Puts all handles which are accessible from given result set into hash
     * set. In some test cases stored data returned as
     * {@link HGRandomAccessResult}. Two results cannot be compared directly. So
     * we put all handles into set and that compare two sets. The order of
     * handles in result set (obtained from database) is difficult to predict.
     */
    public static Set<HGPersistentHandle> set(
            final HGRandomAccessResult<HGPersistentHandle> handles)
    {
        final Set<HGPersistentHandle> allHandles = new HashSet<HGPersistentHandle>();
        while (handles.hasNext())
        {
            allHandles.add(handles.next());
        }
        return allHandles;
    }

    /**
     * Creates temporary file with given prefix and suffix.
     *
     * @return link to the created file instance
     */
    public static File createTempFile(final String prefix, final String suffix)
    {
        File tempFile;
        try
        {
            tempFile = File.createTempFile(prefix, suffix);
        }
        catch (IOException ioException)
        {
            throw new IllegalStateException(ioException);
        }
        return tempFile;
    }

    /**
     * Shortcut for the {@link java.io.File#getCanonicalPath()}. But throws
     * {@link java.lang.IllegalStateException } if something went wrong.
     *
     * @return
     */
    public static String getCanonicalPath(final File file)
    {
        String canonicalPath;
        try
        {
            canonicalPath = file.getCanonicalPath();
        }
        catch (IOException ioException)
        {
            throw new IllegalStateException(ioException);
        }
        return canonicalPath;
    }

    /**
     * Converts from Integer number to appropriate byte array (in terms of
     * HyperGraphDB)
     */
    public static class ByteArrayConverterForInteger implements
            ByteArrayConverter<Integer>
    {
        public byte[] toByteArray(final Integer input)
        {
            final byte[] buffer = new byte[4];
            BAUtils.writeInt(input, buffer, 0);
            return buffer;
        }

        public Integer fromByteArray(final byte[] byteArray, final int offset,
                                     final int length)
        {
            return BAUtils.readInt(byteArray, 0);
        }
    }

    /**
     * Converts from String object number to appropriate byte array (in terms of
     * HyperGraphDB)
     */
    public static class ByteArrayConverterForString implements
            ByteArrayConverter<String>
    {
        public byte[] toByteArray(final String input)
        {
            return BAtoString.getInstance().toByteArray(input);
        }

        public String fromByteArray(final byte[] byteArray, final int offset,
                                    final int length)
        {
            return BAtoString.getInstance().fromByteArray(byteArray, offset,
                    length);
        }
    }

    // TODO: investigate how to compare messages but don't take Sleepycat's
    // TODO: library version into account
    /**
     * Compares two instances which represent exceptions by:
     * <ul>
     * <li>by object's class</li>
     * <li>by message</li>
     * </ul>
     *
     * TestNG assertion are in use.
     */
    public static void assertExceptions(final Exception occurred,
                                        final Exception expected)
    {
        assertEquals(occurred.getClass(), expected.getClass());
        assertEquals(occurred.getMessage(), expected.getMessage());
    }

    /**
     * Verifies that given exception is an instance of certain class. Also
     * verifies that exception contains all specified strings in its message.
     *
     * @param occurred
     *            exception to be verified
     * @param expectedClass
     *            expected class name
     * @param expectedMessageParts
     *            strings than should be contained in the exception's message
     */
    public static void assertExceptions(final Exception occurred,
                                        final Class expectedClass, final String... expectedMessageParts)
    {
        assertEquals(occurred.getClass(), expectedClass);
        final String actualMessage = occurred.getMessage();
        final List<String> parts = Arrays.asList(expectedMessageParts);
        for (final String currentPart : parts)
            assertTrue(actualMessage.contains(currentPart), String.format(
                    "Actual exception's message [%s] does not contain [%s] text.",
                    actualMessage, currentPart));
    }

    /**
     * Returns array like [1..N][1] composed from given [1..N] list.
     * <p>
     * 2D arrays used for providing parameters in test cases written with
     * TestNG.
     */
    public static Object[][] like2DArray(final Class... clazz)
    {
        final int totalItems = clazz.length;
        final Object[][] objects = new Object[totalItems][1];
        for (int i = 0; i < totalItems; i++)
        {
            objects[i][0] = clazz[i];
        }
        return objects;
    }

	/**
	 * Utility method. Puts given data as Integer-Integer pair to database. The
	 * separate transaction is performed.
	 */
	public static void putKeyValuePair(final Environment environment,
			final Database database, final Integer key, final Integer value)
			throws Exception
	{
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(
				transactionForAddingTestData,
				new DatabaseEntry(
						new ByteArrayConverterForInteger()
								.toByteArray(key)),
				new DatabaseEntry(
						new ByteArrayConverterForInteger()
								.toByteArray(value)));
		transactionForAddingTestData.commit();
	}

	/**
	 * Utility method. Puts given data as Integer-String pair using specified
	 * cursor.
	 */
	public static void putKeyValuePair(Cursor realCursor, final Integer key,
			final String value) throws Exception
	{
		realCursor.put(
				new DatabaseEntry(
						new ByteArrayConverterForInteger()
								.toByteArray(key)),
				new DatabaseEntry(
						new ByteArrayConverterForString()
								.toByteArray(value)));
	}
}
