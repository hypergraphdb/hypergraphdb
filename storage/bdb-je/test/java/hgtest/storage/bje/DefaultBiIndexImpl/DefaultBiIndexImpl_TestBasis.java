package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Comparator;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_TestBasis {
    protected final File envHome = new File(System.getProperty("user.home")
            + File.separator + "test_environment");

    protected static final String INDEX_NAME = "sample_index";
    // storage - used only for getting configuration data
    protected final BJEStorageImplementation storage = PowerMock
            .createStrictMock(BJEStorageImplementation.class);
    protected HGTransactionManager transactionManager = PowerMock
            .createStrictMock(HGTransactionManager.class);
    protected ByteArrayConverter<Integer> keyConverter = PowerMock
            .createStrictMock(ByteArrayConverter.class);
    protected ByteArrayConverter<String> valueConverter = PowerMock
            .createStrictMock(ByteArrayConverter.class);
    protected Comparator<?> comparator = PowerMock
            .createStrictMock(Comparator.class);

    // real instances we need: Database, Environment (come from the
    // Sleepycat je library)
    protected EnvironmentConfig config;
    protected Environment environment;

    @BeforeMethod
    protected void resetMocksAndDeleteTestDirectory()
    {
        PowerMock.resetAll();
        deleteTestDirectory();
        startupEnvironment();
    }

    @AfterMethod
    protected void verifyMocksAndDeleteTestDirectory()
    {
        PowerMock.verifyAll();
        shutdownEnvironment();
        deleteTestDirectory();
    }

    protected void deleteTestDirectory()
    {
        final File[] filesInTestDir = envHome.listFiles();
        if (filesInTestDir != null)
        {
            for (final File eachFile : filesInTestDir)
            {
                eachFile.delete();
            }
        }
        envHome.delete();
    }

    protected void startupEnvironment()
    {
        envHome.mkdirs();
        config = new EnvironmentConfig();
        config.setAllowCreate(true).setReadOnly(false).setTransactional(true);
        environment = new Environment(envHome, config);
    }

    protected void shutdownEnvironment()
    {
        environment.close();
    }

    /**
     * Before environment can be closed all opened databases should be closed
     * first. Links to these databases stored in fields of DefaultBiIndexImpl.
     * We obtain them by their names. It is not good. But it seems that there is
     * not way to obtain them from Environment instance.
     */
    protected void closeDatabases(final DefaultBiIndexImpl indexImpl)
            throws NoSuchFieldException, IllegalAccessException
    {
        // one database handle is in DefaultIndexImpl
        final Field firstDatabaseField = indexImpl.getClass().getSuperclass()
                .getDeclaredField("db");
        firstDatabaseField.setAccessible(true);
        final Database firstDatabase = (Database)firstDatabaseField.get(indexImpl);
        // in some test cases first database is not opened, don't close them
        if (firstDatabase != null) {
            firstDatabase.close();
        }
        // another is in DefaultBiIndexImpl
        final Field secondDatabaseField = indexImpl.getClass()
                .getDeclaredField("secondaryDb");
        secondDatabaseField.setAccessible(true);
        final Database secondDatabase = ((Database) secondDatabaseField.get(indexImpl));
        // in some test cases second database is not opened, don't close them
        if (secondDatabase != null) {
            secondDatabase.close();
        }
    }

    // this method is used in most test cases for initializing fake instance of
    // BJEStorageImplementation
    protected void mockStorage()
    {
        EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
        EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
                .times(3);
    }
}
