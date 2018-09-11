package hgtest;

import org.junit.Assert;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.atom.HGAtomSet;
import org.junit.Test;

public class AtomSetTests  extends HGTestBase
{

	@Test
	public void afterAddOneAndClear_ShouldHaveZeroSize()
	{
		final HGAtomSet set = new HGAtomSet();
		HGHandle setHandle = graph.add(set);
		tx(() -> {
			for (int i = 0; i < 10; i++)
			{
				HGHandle atomHandle = graph.add("Atom " + Math.random());
				set.add(atomHandle);
			}
			graph.update(set);
			Assert.assertEquals(10, set.size());
		});
		tx(() -> {
			set.clear();
			Assert.assertEquals(0, set.size());
		});
		reopenDb();
		HGAtomSet setback = graph.get(setHandle);
		Assert.assertEquals(0, setback.size());
	}

}
