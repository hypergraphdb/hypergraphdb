package hgtest.storage.bdb.LinkBinding;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.hypergraphdb.storage.bdb.LinkBinding;
import org.testng.annotations.BeforeMethod;

/**
 * @author Yuriy Sechko
 */
public class LinkBindingTestBasis
{
	protected static final String HANDLE_FACTORY_CLASS_NAME = "org.hypergraphdb.handle.IntHandleFactory";
	protected static final int HANDLE_SIZE = 4;

	protected HGHandleFactory handleFactory;
	protected LinkBinding binding;

	protected byte[] intHandlesAsByteArray(final Integer... intValues)
	{

		final byte[] buffer = new byte[HANDLE_SIZE * intValues.length];
		for (int handleCount = 0; handleCount < intValues.length; handleCount++)
		{
			final HGPersistentHandle currentHandle = new IntPersistentHandle(
					intValues[handleCount]);
			final byte[] currentHandleBytes = currentHandle.toByteArray();
			System.arraycopy(currentHandleBytes, 0, buffer, handleCount
					* HANDLE_SIZE, HANDLE_SIZE);
		}
		return buffer;
	}

	@BeforeMethod
	public void initHandleFactoryAndCreateLinkBinding() throws Exception
	{
		handleFactory = (HGHandleFactory) Class.forName(
				HANDLE_FACTORY_CLASS_NAME).newInstance();
		binding = new LinkBinding(handleFactory);
	}
}
