package hgtest;

import org.hypergraphdb.HGException;

/**
 * <p>
 * This indicates that an exceptional situation occurred during a test. It doesn't
 * indicate test failure, rather it indicates that something else failed during
 * the test and the test would become meaningless if continued. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class TestException extends HGException
{
	static final long serialVersionUID = -1;
	
	public TestException(String msg) { super(msg); }
}