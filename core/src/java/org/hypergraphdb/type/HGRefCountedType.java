package org.hypergraphdb.type;

/**
 * <p>
 * A marker interface indicating that a given {@link HGAtomType} implementation
 * is reference-counted. This means that distinct values of the given type
 * are stored only once in permanent storage and a count is maintained for
 * each separate enclosing type or atom that references them.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface HGRefCountedType
{
}
