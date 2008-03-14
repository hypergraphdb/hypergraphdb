package org.hypergraphdb.conv.types;

import static org.objectweb.asm.Opcodes.AALOAD;
import static org.objectweb.asm.Opcodes.ACC_PROTECTED;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ACONST_NULL;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ARRAYLENGTH;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFNULL;
import static org.objectweb.asm.Opcodes.IF_ICMPLT;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INSTANCEOF;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.NEW;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_5;

import java.beans.EventSetDescriptor;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.swing.DefaultCellEditor;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.conv.DefaultConverter;
import org.hypergraphdb.conv.GenUtils;
import org.hypergraphdb.type.BonesOfBeans;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

public class ClassGenerator
{
	private static final String GENERATED_CLASSES_OUTPUT_DIR = "F:\\temp\\xxx\\";
	private static final String GET_VALUE_DESC = "(Lorg/hypergraphdb/type/Record;Ljava/lang/String;)Ljava/lang/Object;";
	private static final String SET_VALUE_DESC = "(Lorg/hypergraphdb/type/Record;Ljava/lang/String;Ljava/lang/Object;)V";
	public static final String baseClassName = GeneratedClass.class.getName()
			.replace('.', '/');
	protected SwingType type;
	protected HyperGraph hg;
	// generated class Name
	private String genClassName;
	// convenience var to avoid successive replace('.', '/') calls
	private String className;
	// TODO: temporary, testing only
	private SwingTypeIntrospector inspector;
	private static Map<Class<?>, Class<?>> cache = new HashMap<Class<?>, Class<?>>();

	public static Class<?> getClass(Class<?> c) 
	{
		return cache.get(c);
	}

	public ClassGenerator(HyperGraph hg, SwingType type)
	{
		this.hg = hg;
		this.type = type;
		Class<?> cls = type.getJavaClass();
		className = cls.getName().replace('.', '/');
		genClassName = "temp/" + cls.getName().replace('.', '_');
		genClassName = genClassName.replace('$', '_');
		inspector = new SwingTypeIntrospector(hg, type);
	}

	protected void generateCtr(ClassWriter cw)
	{
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null,
				null);
		mv.visitCode();
		Label l0 = new Label();
		mv.visitLabel(l0);
		mv.visitLineNumber(22, l0);
		mv.visitVarInsn(ALOAD, 0);
		mv.visitMethodInsn(INVOKESPECIAL,
				"org/hypergraphdb/conv/types/GeneratedClass", "<init>", "()V");
		mv.visitInsn(RETURN);
		Label l1 = new Label();
		mv.visitLabel(l1);
		mv
				.visitLocalVariable("this", "L" + genClassName + ";", null, l0,
						l1, 0);
		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	public Class<?> generate()
	{
		Class<?> cls = type.getJavaClass();
		ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
		cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, genClassName, null,
				baseClassName, null);
		generateCtr(cw);
		genInstantiate(cw);
		if (!inspector.isEmptyMakeMethod())
			genMake(cw);
		if (!inspector.isEmptyStoreMethod()) 
			genStore(cw);
		cw.visitEnd();
		byte[] byteCode = cw.toByteArray();
		FileOutputStream fos = null;
		try
		{
			fos = new FileOutputStream(GENERATED_CLASSES_OUTPUT_DIR + genClassName
					+ ".class");
			fos.write(byteCode);
			fos.close();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		CGPrivateClassLoader cl = new CGPrivateClassLoader(Thread
				.currentThread().getContextClassLoader());
		Class<?> result = cl.defineClass(genClassName.replace('/', '.'), byteCode);
		cache.put(cls, result);
		return result;
	}

	protected void genMake(ClassWriter cw)
	{
		MethodVisitor mv = cw.visitMethod(ACC_PROTECTED, "makeBean",
				"(Ljava/lang/Object;Lorg/hypergraphdb/type/Record;)V", null,
				null);
		mv.visitCode();
		mv.visitVarInsn(ALOAD, 1);
		mv.visitTypeInsn(CHECKCAST, className);
		mv.visitVarInsn(ASTORE, 3);
		for (String s : inspector.getSettersMap().keySet())
			genMakeSetter(mv, s, inspector.getSettersMap().get(s));
		for (Field f : inspector.getPubFieldsMap().values())
			genMakePubField(mv, f);
		for (Field f : inspector.getPrivFieldsMap().values())
			genMakePrivField(mv, f);
		for (EventSetDescriptor e : inspector.getEventSetDescriptorsMap().values())
			if (e != null) genMakeListeners(mv, e);
		
		mv.visitInsn(RETURN);
		mv.visitMaxs(3, 3);
		mv.visitEnd();
	}

	protected void genStore(ClassWriter cw)
	{
		MethodVisitor mv = cw.visitMethod(ACC_PROTECTED, "storeBean",
				"(Ljava/lang/Object;Lorg/hypergraphdb/type/Record;)V", null,
				null);
		mv.visitCode();
		mv.visitVarInsn(ALOAD, 1);
		mv.visitTypeInsn(CHECKCAST, className);
		mv.visitVarInsn(ASTORE, 3);
		for (String s : inspector.getGettersMap().keySet())
			genStoreGetter(mv, s, inspector.getGettersMap().get(s));
		for (Field f : inspector.getPubFieldsMap().values())
			genStorePubField(mv, f);
		for (String s : inspector.getEventSetDescriptorsMap().keySet())
		{
			EventSetDescriptor e = inspector.getEventSetDescriptorsMap().get(s);
			if (e != null && !filterListenersByType(e.getListenerType()))
				genStoreListeners(mv, s, inspector.getEventSetDescriptorsMap().get(s));
		}
		for (Field f : inspector.getPrivFieldsMap().values())
			genStorePrivField(mv, f);
		mv.visitInsn(RETURN);
		mv.visitMaxs(3, 3);
		mv.visitEnd();
	}

	protected boolean filterListenersByType(Class<?> listenerType)
	{
		if (listenerType == java.awt.event.ComponentListener.class)
		{
			return true;
		}
		// JMenuItems have a change listener added to them in
		// their "add" methods to enable accessibility support -
		// see the add method in JMenuItem for details. We cannot
		// instantiate this instance as it is a private inner class
		// and do not need to do this anyway since it will be created
		// and installed by the "add" method. Special case this for now,
		// ignoring all change listeners on JMenuItems.
		if (listenerType == javax.swing.event.ChangeListener.class
				&& type.getJavaClass() == javax.swing.JMenuItem.class)
		{
			return true;
		}
		return false;
	}

	protected void genStoreListeners(MethodVisitor mv, String name,
			EventSetDescriptor e)
	{
		Method m = e.getGetListenerMethod();
		if (m == null || !m.getReturnType().isArray()) return;
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitLdcInsn(name);
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 1);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitMethodInsn(INVOKEVIRTUAL, className, m.getName(), Type
				.getMethodDescriptor(m));
		mv.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "filterListeners",
		"(Ljava/lang/Object;[Ljava/util/EventListener;)[Ljava/util/EventListener;");
		mv.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "setValue",
				SET_VALUE_DESC);
    }

	protected void genStorePrivField(MethodVisitor mv, Field f)
	{
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitLdcInsn(f.getName());
		mv.visitVarInsn(ALOAD, 3);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Object", "getClass",
				"()Ljava/lang/Class;");
		mv.visitLdcInsn(f.getName());
		mv
				.visitMethodInsn(INVOKESTATIC,
						"org/hypergraphdb/conv/RefUtils",
						"getPrivateFieldValue",
						"(Ljava/lang/Object;Ljava/lang/Class;Ljava/lang/String;)Ljava/lang/Object;");
		mv.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "setValue",
				SET_VALUE_DESC);
	}

	protected void genStorePubField(MethodVisitor mv, Field f)
	{
		Class<?> t = f.getType();
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitLdcInsn(f.getName());
		mv.visitVarInsn(ALOAD, 3);
		if (t.isPrimitive())
		{
			mv.visitFieldInsn(GETFIELD, className, f.getName(), GenUtils
					.getPrimitiveClassDesc(t));
			GenUtils.box(mv, t);
		} else
		{
			mv.visitFieldInsn(GETFIELD, className, f.getName(), 
					Type.getType(t).getDescriptor());
		}
		mv.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "setValue",
				SET_VALUE_DESC);
	}

	protected void genStoreGetter(MethodVisitor mv, String name, Method m)
	{
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitLdcInsn(name);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitMethodInsn(INVOKEVIRTUAL, className, m.getName(), Type
				.getMethodDescriptor(m));
		if (m.getReturnType().isPrimitive())
			GenUtils.box(mv, m.getReturnType());
		mv.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "setValue",
				SET_VALUE_DESC);
	}

	protected void genMakeSetter(MethodVisitor mv, String name, Method m)
	{
		Label l0 = generateBaseGetValueCall(mv, name);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitVarInsn(ALOAD, 5);
		Class<?> t = BonesOfBeans.wrapperEquivalentOf(m.getParameterTypes()[0]);
		String typeName = t.getName();
		mv.visitTypeInsn(CHECKCAST, typeName.replace('.', '/'));
		String desc = GenUtils.getSetterDesc(t);
		if (m.getParameterTypes()[0].isPrimitive())
		{
			GenUtils.unbox(mv, m.getParameterTypes()[0]);
			desc = GenUtils.getSetterDesc(m.getParameterTypes()[0]);
		}
		mv.visitMethodInsn(INVOKEVIRTUAL, className, m.getName(), desc);
		mv.visitLabel(l0);
	}

	protected void genMakePubField(MethodVisitor mv, Field f)
	{
		Label l0 = generateBaseGetValueCall(mv, f.getName());
		mv.visitVarInsn(ALOAD, 3);
		mv.visitVarInsn(ALOAD, 5);
		Class<?> t = f.getType();
		if (t.isPrimitive())
		{
			String typeName = BonesOfBeans.wrapperEquivalentOf(t).getName()
					.replace('.', '/');
			mv.visitTypeInsn(CHECKCAST, typeName);
			GenUtils.unbox(mv, t);
			mv.visitFieldInsn(PUTFIELD, className, f.getName(), GenUtils
					.getPrimitiveClassDesc(t));
		} else
		{
			String tt = t.getName().replace('.', '/');
			mv.visitTypeInsn(CHECKCAST, tt);
			mv.visitFieldInsn(PUTFIELD, className, f.getName(), Type.getType(t).getDescriptor());
	}
		mv.visitLabel(l0);
	}
	protected void genMakePrivField(MethodVisitor mv, Field f){
		Label l0 = generateBaseGetValueCall(mv, f.getName());
		Label l9 = new Label();
		mv.visitLabel(l9);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Object", "getClass", "()Ljava/lang/Class;");
		Label l10 = new Label();
		mv.visitLabel(l10);
		mv.visitVarInsn(ALOAD, 4);
		mv.visitVarInsn(ALOAD, 5);
		Label l11 = new Label();
		mv.visitLabel(l11);
        mv.visitMethodInsn(INVOKESTATIC, "org/hypergraphdb/conv/RefUtils", "setPrivateFieldValue", 
        		 "(Ljava/lang/Object;Ljava/lang/Class;Ljava/lang/String;Ljava/lang/Object;)V");
		mv.visitLabel(l0);
	}

	protected void genMakeListeners(MethodVisitor mv, EventSetDescriptor e)
	{
		// System.out.println("Listener: " + className + ":" + e.getName());
		Label l0 = generateBaseGetValueCall(mv, e.getName()
				+ DefaultConverter.LISTENERS_KEY);
		mv.visitVarInsn(ALOAD, 5);
		mv.visitTypeInsn(CHECKCAST, "[Ljava/util/EventListener;");
		mv.visitVarInsn(ASTORE, 9);
		mv.visitInsn(ICONST_0);
		mv.visitVarInsn(ISTORE, 7);
		mv.visitVarInsn(ALOAD, 9);
		mv.visitInsn(ARRAYLENGTH);
		mv.visitVarInsn(ISTORE, 8);
		Label l1 = new Label();
		mv.visitJumpInsn(GOTO, l1);
		Label l2 = new Label();
		mv.visitLabel(l2);
		mv.visitVarInsn(ALOAD, 9);
		mv.visitVarInsn(ILOAD, 7);
		mv.visitInsn(AALOAD);
		mv.visitVarInsn(ASTORE, 6);
		mv.visitVarInsn(ALOAD, 3);
		mv.visitVarInsn(ALOAD, 6);
		mv.visitTypeInsn(CHECKCAST, e.getListenerType().getName().replace('.',
				'/')); // "java/awt/event/ActionListener");
		Method m = e.getAddListenerMethod();
		mv.visitMethodInsn(INVOKEVIRTUAL, className, m.getName(), Type
				.getMethodDescriptor(m));
		mv.visitIincInsn(7, 1);
		mv.visitLabel(l1);
		mv.visitVarInsn(ILOAD, 7);
		mv.visitVarInsn(ILOAD, 8);
		mv.visitJumpInsn(IF_ICMPLT, l2);
		mv.visitLabel(l0);
	}

	protected static Label generateBaseGetValueCall(MethodVisitor mv,
			String name)
	{
		mv.visitLdcInsn(name);
		mv.visitVarInsn(ASTORE, 4);
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitVarInsn(ALOAD, 4);
		mv.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "getValue",
				GET_VALUE_DESC);
		mv.visitVarInsn(ASTORE, 5);
		mv.visitVarInsn(ALOAD, 5);
		Label l0 = new Label();
		mv.visitJumpInsn(IFNULL, l0);
		return l0;
	}

	protected void genInstantiate(ClassWriter cw)
	{
		if(DefaultCellEditor.class.isAssignableFrom(type.getJavaClass()))
		{
			genDefaultCellEditorCtr(cw);
			return;
		}
		Object ctr_or_m = GenUtils.getConstructor(hg, type);
		Constructor<?> ctr = (ctr_or_m instanceof Constructor<?>) ? 
				(Constructor<?>) ctr_or_m : null;
		MethodVisitor mv = cw.visitMethod(ACC_PROTECTED, "instantiate",
				"(Lorg/hypergraphdb/type/Record;)Ljava/lang/Object;", null,
				null);
		mv.visitCode();
		
		if (ctr_or_m == null ||
				(ctr != null && ctr.getParameterTypes().length == 0))
		{
			mv.visitTypeInsn(NEW, className);
			mv.visitInsn(DUP);
			mv.visitMethodInsn(INVOKESPECIAL, className, "<init>", "()V");
		}else 
		{
			Method m = (ctr != null) ? null :(Method) ctr_or_m;
			String[] names = GenUtils.getCtrSlotNames(hg, type);
			Class<?>[] types = (ctr != null) ? 
					ctr.getParameterTypes() : m.getParameterTypes();
			if(ctr != null)	
			{
				mv.visitTypeInsn(NEW, className);
				mv.visitInsn(DUP);
			}
			for (int i = 0; i < names.length; i++)
				genInstVar(mv, names[i], types[i]);
			if(ctr != null)
			  mv.visitMethodInsn(INVOKESPECIAL, className, "<init>", Type
					.getConstructorDescriptor(ctr));
			else{
				mv.visitMethodInsn(INVOKESTATIC, Type.getType(
						m.getDeclaringClass()).getInternalName(),
						m.getName(), Type.getMethodDescriptor(m));
			}
		}
		mv.visitInsn(ARETURN);
		mv.visitMaxs(6, 2);
		mv.visitEnd();
	}

	protected void genInstVar(MethodVisitor mv, String name, Class<?> c)
	{
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 1);
		mv.visitLdcInsn(name);
		mv
				.visitMethodInsn(INVOKEVIRTUAL, baseClassName, "getValue",
						"(Lorg/hypergraphdb/type/Record;Ljava/lang/String;)Ljava/lang/Object;");
		if (c.isPrimitive())
		{
			mv.visitTypeInsn(CHECKCAST, GenUtils.getWrapType(c)
					.getInternalName());
			GenUtils.unbox(mv, c);
		} else
			mv.visitTypeInsn(CHECKCAST, Type.getType(c).getInternalName());
	}

//	TODO:// move to some interface providing for plug support
//	Object o = getValue(rec, "editorComponent");
//	if (o instanceof JCheckBox)
//		return new DefaultCellEditor((JCheckBox) o);
//	else if (o instanceof JComboBox)
//		return new DefaultCellEditor((JComboBox) o);
//	else if (o instanceof JTextField)
//		return new DefaultCellEditor((JTextField) o);
//	return null;
	private void genDefaultCellEditorCtr(ClassWriter cw)
	{
		MethodVisitor mv = cw.visitMethod(ACC_PROTECTED, "instantiate",
				"(Lorg/hypergraphdb/type/Record;)Ljava/lang/Object;", null,
				null);
		mv.visitCode();
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 1);
		mv.visitLdcInsn("editorComponent");
		mv
				.visitMethodInsn(INVOKEVIRTUAL, className,
						"getValue",
						"(Lorg/hypergraphdb/type/Record;Ljava/lang/String;)Ljava/lang/Object;");
		mv.visitVarInsn(ASTORE, 2);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(INSTANCEOF, "javax/swing/JCheckBox");
		Label l0 = new Label();
		mv.visitJumpInsn(IFEQ, l0);
		mv.visitTypeInsn(NEW, "javax/swing/DefaultCellEditor");
		mv.visitInsn(DUP);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(CHECKCAST, "javax/swing/JCheckBox");
		mv.visitMethodInsn(INVOKESPECIAL, "javax/swing/DefaultCellEditor",
				"<init>", "(Ljavax/swing/JCheckBox;)V");
		mv.visitInsn(ARETURN);
		mv.visitLabel(l0);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(INSTANCEOF, "javax/swing/JComboBox");
		Label l1 = new Label();
		mv.visitJumpInsn(IFEQ, l1);
		mv.visitTypeInsn(NEW, "javax/swing/DefaultCellEditor");
		mv.visitInsn(DUP);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(CHECKCAST, "javax/swing/JComboBox");
		mv.visitMethodInsn(INVOKESPECIAL, "javax/swing/DefaultCellEditor",
				"<init>", "(Ljavax/swing/JComboBox;)V");
		mv.visitInsn(ARETURN);
		mv.visitLabel(l1);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(INSTANCEOF, "javax/swing/JTextField");
		Label l2 = new Label();
		mv.visitJumpInsn(IFEQ, l2);
		mv.visitTypeInsn(NEW, "javax/swing/DefaultCellEditor");
		mv.visitInsn(DUP);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(CHECKCAST, "javax/swing/JTextField");
		mv.visitMethodInsn(INVOKESPECIAL, "javax/swing/DefaultCellEditor",
				"<init>", "(Ljavax/swing/JTextField;)V");
		mv.visitInsn(ARETURN);
		mv.visitLabel(l2);
		mv.visitInsn(ACONST_NULL);
		mv.visitInsn(ARETURN);
		mv.visitMaxs(3, 3);
		mv.visitEnd();
	}

	private static class CGPrivateClassLoader extends ClassLoader
	{
		public CGPrivateClassLoader(ClassLoader parent)
		{
			super(parent);
		}

		public CGPrivateClassLoader()
		{
		}

		public Class<?> defineClass(final String name, final byte[] b)
		{
			return defineClass(name, b, 0, b.length);
		}
	}
}
