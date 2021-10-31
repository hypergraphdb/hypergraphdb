package hgtest.links;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class SampleLink1 extends HGPlainLink
{
	public SampleLink1(HGHandle...targets)
	{
		super(targets);
	}
}
