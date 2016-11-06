package org.hypergraphdb.storage.lmdb;

public class test {
public static void main(String[] args) {
	String foo = "a//b";
	foo = foo.replaceAll("//", "");
	System.out.println(foo);
}
}
