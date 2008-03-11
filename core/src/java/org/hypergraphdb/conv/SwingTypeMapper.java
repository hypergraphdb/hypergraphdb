package org.hypergraphdb.conv;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.atom.HGSerializable;
import org.hypergraphdb.conv.types.ClassGenerator;
import org.hypergraphdb.conv.types.GeneratedClass;
import org.hypergraphdb.conv.types.SwingBinding;
import org.hypergraphdb.conv.types.SwingType;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.type.HGAbstractType;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.JavaAbstractBinding;
import org.hypergraphdb.type.JavaInterfaceBinding;
import org.hypergraphdb.type.JavaObjectBinding;
import org.hypergraphdb.type.JavaTypeFactory;
import org.hypergraphdb.type.JavaTypeMapper;
import org.hypergraphdb.type.RecordType;
import org.hypergraphdb.util.HGUtils;

public class SwingTypeMapper implements JavaTypeMapper {
	private HyperGraph graph = null;
	// private HashSet<String> classes = null;
	private HGIndex<String, HGPersistentHandle> idx = null;

	// private HGIndex<String, HGPersistentHandle> getIndex() {
	// if (idx == null) {
	// HGHandle t = graph.getTypeSystem().getTypeHandle(
	// HGSerializable.class);
	// HGIndexer indexer = new ByPartIndexer(t, "classname");
	// idx = graph.getIndexManager().getIndex(indexer);
	// if (idx == null) {
	// idx = graph.getIndexManager().register(indexer);
	// }
	// return idx;
	// }
	// return idx;
	// }

	public void setHyperGraph(HyperGraph graph) {
		this.graph = graph;
	}

	public HGAtomType defineHGType(Class<?> javaClass, HGHandle typeHandle) {
		if (javaClass.getName().startsWith("javax")
				|| javaClass.getName().startsWith("java.awt")) {

			SwingType type = new SwingType(javaClass);
			type.setHyperGraph(graph);
			type.init(typeHandle);
			return type;
		}
		return null;
	}

	public HGAtomType getJavaBinding(HGHandle typeHandle, HGAtomType hgType,
			Class<?> javaClass) {
		if (hgType instanceof SwingType) {
			if (Modifier.isPublic(javaClass.getModifiers())) {

				try {
					Class<?> gen = ClassGenerator.getClass(javaClass);
					if (gen == null)
						gen = new ClassGenerator(graph, (SwingType) hgType)
								.generate();
					GeneratedClass inst = (GeneratedClass) gen.newInstance();
					inst.setHgType((SwingType) hgType);
					inst.setTypeHandle(typeHandle);
					// inst.setHyperGraph(graph);
					return inst;
				} catch (Throwable ex) {
					System.err.println(ex);
					ex.printStackTrace();
				}
			}
			return new SwingBinding(typeHandle, (SwingType) hgType);
		}

		return null;
	}

}
