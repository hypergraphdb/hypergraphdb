package hgtest.storage.bje;

import com.sleepycat.je.*;

/**
 * @author Yuriy Sechko
 */
public class TestUtils
{
	/**
	 * Utility method. Puts given data as Integer-Integer pair to database. The
	 * separate transaction is performed.
	 */
	public static void putKeyValuePair(final Environment environment,
			final Database database, final Integer key, final Integer value)
	{
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(
				transactionForAddingTestData,
				new DatabaseEntry(new hgtest.TestUtils.ByteArrayConverterForInteger()
						.toByteArray(key)),
				new DatabaseEntry(new hgtest.TestUtils.ByteArrayConverterForInteger()
						.toByteArray(value)));
		transactionForAddingTestData.commit();
	}

	/**
	 * Utility method. Puts given data as Integer-String pair to cursor.
	 */
	public static void putKeyValuePair(Cursor realCursor, final Integer key,
			final String value)
	{
		realCursor.put(
				new DatabaseEntry(new hgtest.TestUtils.ByteArrayConverterForInteger()
						.toByteArray(key)),
				new DatabaseEntry(new hgtest.TestUtils.ByteArrayConverterForString()
						.toByteArray(value)));
	}
}
