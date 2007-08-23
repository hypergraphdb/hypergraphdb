package org.hypergraphdb.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 
 * <p>
 * This annotation can be used to mark a bean property to be ignored
 * when a HyperGraph type is being automatically created for it. 
 * </p>
 *
 * <p>
 * In general, a bean property is included as a record slot in HyperGraph whenever
 * both a setter and a getter are present for it. You can annotate either the setter,
 * getter, or the member field declaration with <code>@HGIgnore</code> in order to
 * indicate that the property it NOT to be recorded in HyperGraph. 
 * </p>
 * 
 * <p>
 * The most common case where a bean property is to be ignored is when a 
 * getter/setter combination does not actually correspond to some independent
 * internal bean field.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HGIgnore
{
}
