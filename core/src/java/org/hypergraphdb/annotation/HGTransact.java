package org.hypergraphdb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * This annotation indicates that a given method should be wrapped in a transaction. It is
 * intended to be used in cases where Java methods are being called reflexively and the caller
 * can make the invocation within the context of a HyperGraphDB transaction. Note that as with
 * all transactions in HyperGraphDB, such transaction methods should be "repeatable". That is,
 * they should not have any side-effects that affect non-transactional structures This is because
 * the call may be repeated in case of a transaction database conflict.
 * </p>
 * 
 * @author borislav
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HGTransact
{
	/**
	 * Use "read" to specify that the transaction is read-only or "write" to specify that
	 * it is read-write.
	 */
	String value();
}