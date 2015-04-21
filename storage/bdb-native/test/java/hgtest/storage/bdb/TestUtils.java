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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * Some helper methods to deal with BDB specific classes.
 * 
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
			throws Exception
	{
		final Transaction transactionForAddingTestData = environment
				.beginTransaction(null, null);
		database.put(
				transactionForAddingTestData,
				new DatabaseEntry(
						new hgtest.TestUtils.ByteArrayConverterForInteger()
								.toByteArray(key)),
				new DatabaseEntry(
						new hgtest.TestUtils.ByteArrayConverterForInteger()
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
						new hgtest.TestUtils.ByteArrayConverterForInteger()
								.toByteArray(key)),
				new DatabaseEntry(
						new hgtest.TestUtils.ByteArrayConverterForString()
								.toByteArray(value)));
	}
}
