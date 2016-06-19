package hgtest.storage.bje.LinkBinding;

import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.hypergraphdb.storage.bje.LinkBinding;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static java.lang.System.arraycopy;
import static org.junit.rules.ExpectedException.none;

public class LinkBindingTestBasis
{
	protected static final String HANDLE_FACTORY_CLASS_NAME = "org.hypergraphdb.handle.IntHandleFactory";
	protected static final int HANDLE_SIZE = 4;

	@Rule
	public final ExpectedException below = none();

	protected HGHandleFactory handleFactory;
	protected LinkBinding binding;

	protected static byte[] intHandlesAsByteArray(final Integer... intValues)
	{
		final byte[] buffer = new byte[HANDLE_SIZE * intValues.length];
		for (int handleCount = 0; handleCount < intValues.length; handleCount++)
		{
			final HGPersistentHandle currentHandle = new IntPersistentHandle(
					intValues[handleCount]);
			final byte[] currentHandleBytes = currentHandle.toByteArray();
			arraycopy(currentHandleBytes, 0, buffer, handleCount * HANDLE_SIZE,
					HANDLE_SIZE);
		}
		return buffer;
	}

	@Before
	public void initHandleFactoryAndCreateLinkBinding() throws Exception
	{
		handleFactory = (HGHandleFactory) Class.forName(
				HANDLE_FACTORY_CLASS_NAME).newInstance();
		binding = new LinkBinding(handleFactory);
	}
}
