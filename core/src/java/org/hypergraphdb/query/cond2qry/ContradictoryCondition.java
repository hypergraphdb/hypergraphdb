package org.hypergraphdb.query.cond2qry;

/**
 * <p>
 * To simplify the logic of query compilation a bit - a step during the compilation process can throw that exception to indicate
 * that it has detected a condition that is self-contradictory, it cannot be fullfilled and it will always lead to an empty
 * result set.
 * </p>
 * 
 * @author biordanov
 *
 */
public class ContradictoryCondition extends RuntimeException 
{
}