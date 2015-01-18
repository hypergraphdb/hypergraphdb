package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.*;
import hgtest.storage.bje.IndexImplTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.*;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Comparator;

import static hgtest.storage.bje.TestUtils.deleteDirectory;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImplTestBasis extends IndexImplTestBasis
{
	protected static final String SECONDARY_DATABASE_FIELD_NAME = "secondaryDb";

	/**
	 * Before environment can be closed all opened databases should be closed
	 * first. Links to these databases stored in fields of DefaultBiIndexImpl.
	 * We obtain them by their names. It is not good. But it seems that there is
	 * not way to obtain them from Environment instance.
	 */
	protected void closeDatabases(final DefaultBiIndexImpl indexImpl)
			throws NoSuchFieldException, IllegalAccessException
	{
		// close database in DefaultIndexImpl
		final Field firstDatabaseField = indexImpl.getClass().getSuperclass()
				.getDeclaredField(DATABASE_FIELD_NAME);
		firstDatabaseField.setAccessible(true);
		final Database firstDatabase = (Database) firstDatabaseField
				.get(indexImpl);
		// in some test cases first database is not opened, don't close them
		if (firstDatabase != null)
		{
			firstDatabase.close();
		}
		// another is in DefaultBiIndexImpl
		final Field secondDatabaseField = indexImpl.getClass()
				.getDeclaredField(SECONDARY_DATABASE_FIELD_NAME);
		secondDatabaseField.setAccessible(true);
		final Database secondDatabase = ((Database) secondDatabaseField
				.get(indexImpl));
		// in some test cases second database is not opened, don't close them
		if (secondDatabase != null)
		{
			secondDatabase.close();
		}
	}

	protected void mockStorage()
	{
		EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
				.times(3);
	}
}
