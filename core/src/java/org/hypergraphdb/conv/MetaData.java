package org.hypergraphdb.conv;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Choice;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagLayout;
import java.awt.Menu;
import java.awt.MenuBar;
import java.awt.MenuItem;
import java.awt.MenuShortcut;
import java.awt.Point;
import java.awt.Rectangle;
import java.beans.BeanInfo;
import java.beans.Encoder;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.Box;
import javax.swing.DefaultCellEditor;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.KeyStroke;
import javax.swing.ToolTipManager;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import org.hypergraphdb.HGException;
import org.hypergraphdb.conv.Converter.AddOnType;
import org.hypergraphdb.conv.types.AddOnFactory;
import org.hypergraphdb.conv.types.GeneratedClass;
import org.hypergraphdb.type.Record;

/*
 * Like the <code>Intropector</code>, the <code>MetaData</code> class
 * contains <em>meta</em> objects that describe the way
 * classes should express their state in terms of their
 * own public APIs.
 *
 * @see java.beans.Intropector
 *
 * @version 1.39 05/05/04
 * @author Philip Milne
 * @author Steve Langley
 */
public class MetaData {
	
	private static final String PACKAGE_NAME = "org.hypergraphdb.conv.";
	private static Map<String, Converter> internalPersistenceDelegates = new HashMap<String, Converter>();
	private static Map<String, Vector> transientProperties = new HashMap<String, Vector>();

	static {
		//TODO: throws IllegalArgExc if > 0 and set before label
		removeProperty("javax.swing.AbstractButton", "displayedMnemonicIndex");
		removeProperty("javax.swing.JList","UI");
		removeProperty("javax.swing.JScrollBar","UI");
		removeProperty("javax.swing.JScrollPane","UI");
		// Transient properties
		// awt
		// Infinite graphs.
		removeProperty("java.awt.geom.RectangularShape", "frame");
		removeProperty("java.awt.Rectangle2D", "frame");
		removeProperty("java.awt.Rectangle", "frame");
		removeProperty("java.awt.geom.Rectangle2D", "frame");
		removeProperty("java.awt.geom.Rectangle2D.Double", "frame");
		removeProperty("java.awt.geom.Rectangle2D.Float", "frame");
		removeProperty("java.awt.Rectangle", "bounds");
		removeProperty("java.awt.Dimension", "size");
		removeProperty("java.awt.Point", "location");
		// The color and font properties in Component need special treatment,
		// see above.
		removeProperty("java.awt.Component", "foreground");
		removeProperty("java.awt.Component", "background");
		removeProperty("java.awt.Component", "font");
		// The visible property of Component needs special treatment because of
		// Windows.
		removeProperty("java.awt.Component", "visible");
		// This property throws an exception if accessed when there is no child.
		removeProperty("java.awt.ScrollPane", "scrollPosition");
		// 4917458 this should be removed for XAWT since it may throw
		// an unsupported exception if there isn't any input methods.
		// This shouldn't be a problem since these are added behind
		// the scenes automatically.
		removeProperty("java.awt.im.InputContext", "compositionEnabled");
		// swing
		// The size properties in JComponent need special treatment, see above.
		removeProperty("javax.swing.JComponent", "minimumSize");
		removeProperty("javax.swing.JComponent", "preferredSize");
		removeProperty("javax.swing.JComponent", "maximumSize");
		// These properties have platform specific implementations
		// and should not appear in archives.
		removeProperty("javax.swing.ImageIcon", "image");
		removeProperty("javax.swing.ImageIcon", "imageObserver");
		// This property throws an exception when set in JMenu.
		// PENDING: Note we must delete the property from
		// the superclass even though the superclass's
		// implementation does not throw an error.
		// This needs some more thought.
		removeProperty("javax.swing.JMenu", "accelerator");
		removeProperty("javax.swing.JMenu", "delay");
		//removeProperty("javax.swing.JMenuItem", "accelerator");
		// This property unconditionally throws a "not implemented" exception.
		removeProperty("javax.swing.JMenuBar", "helpMenu");
		// XXX
		removeProperty("javax.swing.JMenu", "UI");
		removeProperty("javax.swing.JMenuBar", "UI");
		removeProperty("javax.swing.JMenuBar", "layout");
		removeProperty("javax.swing.JMenuItem", "UI");
		removeProperty("javax.swing.JButton", "UI");

		// The scrollBars in a JScrollPane are dynamic and should not
		// be archived. The row and columns headers are changed by
		// components like JTable on "addNotify".
		removeProperty("javax.swing.JScrollPane", "verticalScrollBar");
		removeProperty("javax.swing.JScrollPane", "horizontalScrollBar");
		removeProperty("javax.swing.JScrollPane", "rowHeader");
		removeProperty("javax.swing.JScrollPane", "columnHeader");
		removeProperty("javax.swing.JViewport", "extentSize");
		// Renderers need special treatment, since their properties
		// change during rendering.
		removeProperty("javax.swing.table.JTableHeader", "defaultRenderer");
		removeProperty("javax.swing.JList", "cellRenderer");
		removeProperty("javax.swing.JList", "selectedIndices");
		// The lead and anchor selection indexes are best ignored.
		// Selection is rarely something that should persist from
		// development to deployment.
		removeProperty("javax.swing.DefaultListSelectionModel",
				"leadSelectionIndex");
		removeProperty("javax.swing.DefaultListSelectionModel",
				"anchorSelectionIndex");
		// The selection must come after the text itself.
		removeProperty("javax.swing.JComboBox", "selectedIndex");
		// All selection information should come after the JTabbedPane is built
		removeProperty("javax.swing.JTabbedPane", "selectedIndex");
		removeProperty("javax.swing.JTabbedPane", "selectedComponent");
		// PENDING: The "disabledIcon" property is often computed from the icon
		// property.
		removeProperty("javax.swing.AbstractButton", "disabledIcon");
		removeProperty("javax.swing.JLabel", "disabledIcon");
		// The caret property throws errors when it it set beyond
		// the extent of the text. We could just set it after the
		// text, but this is probably not something we want to archive anyway.
		removeProperty("javax.swing.text.JTextComponent", "caret");
		removeProperty("javax.swing.text.JTextComponent", "caretPosition");
		// The selectionStart must come after the text itself.
		removeProperty("javax.swing.text.JTextComponent", "selectionStart");
		removeProperty("javax.swing.text.JTextComponent", "selectionEnd");
		
		//removeProperty("javax.swing.plaf.basic.LazyActionMap", "parent");
	
		removeProperty("javax.swing.JRootPane", "layout");
		removeProperty("javax.swing.JFrame", "layout");
		removeProperty("javax.swing.JFrame", "layeredPane");
		removeProperty("javax.swing.JFrame", "glassPane");
		removeProperty("javax.swing.JRootPane", "contentPane");
		removeProperty("javax.swing.JFrame", "menuBar");
		removeProperty("javax.swing.JRootPane", "JMenuBar");
		removeProperty("javax.swing.JRootPane", "layeredPane");
		removeProperty("javax.swing.JRootPane", "glassPane");
		
		//removeProperty("javax.swing.JFrame", "menuBar");
		
		registerConstructor("javax.swing.DefaultCellEditor",  
				new String[]{"editorComponent"}, new Class[]{JCheckBox.class});
		
		registerConstructor("javax.swing.Box", new String[] {"layoutMgr.axis"}, new Class[]{Integer.TYPE});
		registerFactoryConstructor(KeyStroke.class,  KeyStroke.class, "getKeyStroke",
				new String[]{"keyCode", "modifiers"},
				new Class[]{Integer.TYPE, Integer.TYPE});
		registerFactoryConstructor(ToolTipManager.class, ToolTipManager.class, 
				"sharedInstance", new String[0], new Class[0]);
		registerConstructor("javax.swing.plaf.basic.LazyActionMap",
				new String[] { "_loader" }, new Class[] { Class.class });

		registerConstructor("javax.swing.plaf.InsetsUIResource", new String[] {
				"top", "left", "bottom", "right" });

		registerConstructor("java.awt.MenuShortcut", new String[] { "key",
				"usesShift" });
		registerConstructor("javax.swing.plaf.IconUIResource",
				new String[] { "delegate" });
		// Constructors.
		// util
		registerConstructor("java.util.Date", new String[] { "time" });
		// beans
		// TODO: should write converters fo these two if ever needed
		// registerConstructor("java.beans.Statement", new String[] { "target",
		// "methodName", "arguments" });
		// registerConstructor("java.beans.Expression", new String[] { "target",
		// "methodName", "arguments" });
		//
		registerConstructor("java.beans.EventHandler", new String[] { "target",
				"action", "eventPropertyName", "listenerMethodName" });
		// awt
		registerConstructor("java.awt.Point", new String[] { "x", "y" });
		registerConstructor("java.awt.Dimension", new String[] { "width",
				"height" });
		registerConstructor("java.awt.Rectangle", new String[] { "x", "y",
				"width", "height" });
		registerConstructor("java.awt.Insets", new String[] { "top", "left",
				"bottom", "right" });
		registerConstructor("java.awt.Color", new String[] { "rGB" });
		// { "red", "green", "blue", "alpha" });
		registerConstructor("java.awt.Font", new String[] { "name", "style",
				"size" });
		registerConstructor("java.awt.Cursor", new String[] { "type" });
		//registerConstructor("java.awt.GridBagConstraints", new String[] {
		//		"gridx", "gridy", "gridwidth", "gridheight", "weightx",
		//		"weighty", "anchor", "fill", "insets", "ipadx", "ipady" });
		registerConstructor("java.awt.ScrollPane",
				new String[] { "scrollbarDisplayPolicy" });
		// swing
		registerConstructor("javax.swing.plaf.FontUIResource", new String[] {
				"name", "style", "size" });
		registerConstructor("javax.swing.plaf.ColorUIResource", new String[] {
				"red", "green", "blue" });
		registerConstructor("javax.swing.tree.DefaultTreeModel",
				new String[] { "root" }, new Class[]{TreeNode.class});
		registerConstructor("javax.swing.JTree", new String[] { "model" });
		registerConstructor("javax.swing.tree.TreePath",
				new String[] { "path" });
		registerConstructor("javax.swing.OverlayLayout",
				new String[] { "target" });
		registerConstructor("javax.swing.BoxLayout", new String[] { "target",
				"axis" });
		registerConstructor("javax.swing.Box$Filler", new String[] {
				"minimumSize", "preferredSize", "maximumSize" });
		/*
		 * This is required because the JSplitPane reveals a private layout
		 * class called BasicSplitPaneUI$BasicVerticalLayoutManager which
		 * changes with the orientation. To avoid the necessity for
		 * instantiating it we cause the orientation attribute to get set before
		 * the layout manager - that way the layout manager will be changed as a
		 * side effect. Unfortunately, the layout property belongs to the
		 * superclass and therefore precedes the orientation property. PENDING -
		 * we need to allow this kind of modification. For now, put the property
		 * in the constructor.
		 */
		registerConstructor("javax.swing.JSplitPane",
				new String[] { "orientation" });
		// Try to synthesize the ImageIcon from its description.
		// XXX
		// registerConstructor("javax.swing.ImageIcon",
		// new String[] { "description" });
		// JButton's "label" and "actionCommand" properties are related,
		// use the label as a constructor argument to ensure that it is set
		// first.
		// This remove the benign, but unnecessary, manipulation of
		// actionCommand
		// property in the common case.
		registerConstructor("javax.swing.JButton", new String[] { "label" });
		// borders
		registerConstructor("javax.swing.border.BevelBorder", new String[] {
				"bevelType", "highlightOuter", "highlightInner", "shadowOuter",
				"shadowInner" });
		registerConstructor("javax.swing.plaf.BorderUIResource",
				new String[] { "delegate" });
		registerConstructor(
				"javax.swing.plaf.BorderUIResource$BevelBorderUIResource",
				new String[] { "bevelType", "highlightOuter", "highlightInner",
						"shadowOuter", "shadowInner" });
		registerConstructor("javax.swing.border.CompoundBorder", new String[] {
				"outsideBorder", "insideBorder" });
		registerConstructor(
				"javax.swing.plaf.BorderUIResource$CompoundBorderUIResource",
				new String[] { "outsideBorder", "insideBorder" });
		registerConstructor("javax.swing.border.EmptyBorder", new String[] {
				"top", "left", "bottom", "right" });
		registerConstructor(
				"javax.swing.plaf.BorderUIResource$EmptyBorderUIResource",
				new String[] { "top", "left", "bottom", "right" });
		registerConstructor("javax.swing.border.EtchedBorder", new String[] {
				"etchType", "highlight", "shadow" });
		registerConstructor(
				"javax.swing.plaf.BorderUIResource$EtchedBorderUIResource",
				new String[] { "etchType", "highlight", "shadow" });
		registerConstructor("javax.swing.border.LineBorder", new String[] {
				"lineColor", "thickness" });
		registerConstructor(
				"javax.swing.plaf.BorderUIResource$LineBorderUIResource",
				new String[] { "lineColor", "thickness" });
		// Note this should check to see which of "color" and "tileIcon" is
		// non-null.
		registerConstructor("javax.swing.border.MatteBorder", new String[] {
				"top", "left", "bottom", "right", "tileIcon" });
		registerConstructor(
				"javax.swing.plaf.BorderUIResource$MatteBorderUIResource",
				new String[] { "top", "left", "bottom", "right", "tileIcon" });
		registerConstructor("javax.swing.border.SoftBevelBorder", new String[] {
				"bevelType", "highlightOuter", "highlightInner", "shadowOuter",
				"shadowInner" });
		// registerConstructorWithBadEqual("javax.swing.plaf.BorderUIResource$SoftBevelBorderUIResource",
		// new String[]{"bevelType", "highlightOuter", "highlightInner",
		// "shadowOuter", "shadowInner"});
		registerConstructor("javax.swing.border.TitledBorder", new String[] {
				"border", "title", "titleJustification", "titlePosition",
				"titleFont", "titleColor" });

		registerConstructor(
				"javax.swing.plaf.BorderUIResource$TitledBorderUIResource",
				new String[] { "border", "title", "titleJustification",
						"titlePosition", "titleFont", "titleColor" });
		

	}

	/* pp */static boolean equals(Object o1, Object o2) {
		return (o1 == null) ? (o2 == null) : o1.equals(o2);
	}

	// Entry points for Encoder.
	public synchronized static void setConverter(Class type,
			Converter persistenceDelegate) {
		setBeanAttribute(type, "persistenceDelegate", persistenceDelegate);
	}

	public synchronized static Converter getConverter(Class type) {
		if (type == null)
			return null;

		String typeName = type.getName();
		// System.out.println("Converter: " + typeName);
			
		// Check to see if there are properties that have been lazily registered
		// for removal.
		if (getBeanAttribute(type, "transient_init") == null) 
		{
			Vector tp = (Vector) transientProperties.get(typeName);
			if (tp != null) {
				for (int i = 0; i < tp.size(); i++) {
					// System.out.println("TransientProperties: " + tp.get(i));
					setPropertyAttribute(type, (String) tp.get(i), "transient",
							Boolean.TRUE);
				}
			}
			setBeanAttribute(type, "transient_init", Boolean.TRUE);
		}
		Converter pd = (Converter) getBeanAttribute(type, "persistenceDelegate");
		if (pd == null) {
			pd = internalPersistenceDelegates.get(typeName);
			if (pd != null) {
				return pd;
			}
			internalPersistenceDelegates.put(typeName, new DefaultConverter(
					typeName, null));
			try {
				String name = type.getName();
				Class c = Class.forName(PACKAGE_NAME + name.replace('.', '_')
						+ "_PersistenceDelegate");
				pd = (Converter) c.newInstance();
				internalPersistenceDelegates.put(typeName, pd);
			} catch (ClassNotFoundException e) {
			} catch (Exception e) {
				System.err.println("Internal error: " + e);
				e.printStackTrace();
			}
		}
		return (pd != null) ? pd : internalPersistenceDelegates.get(typeName);
	}

	// Wrapper for Introspector.getBeanInfo to handle exception handling.
	// Note: this relys on new 1.4 Introspector semantics which cache the
	// BeanInfos
	public static BeanInfo getBeanInfo(Class type) {
		BeanInfo info = null;
		try {
			info = Introspector.getBeanInfo(type);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return info;
	}

	private static PropertyDescriptor getPropertyDescriptor(Class type,
			String propertyName) {
		BeanInfo info = getBeanInfo(type);
		PropertyDescriptor[] propertyDescriptors = info
				.getPropertyDescriptors();
		// System.out.println("Searching for: " + propertyName + " in " + type);
		for (int i = 0; i < propertyDescriptors.length; i++) {
			PropertyDescriptor pd = propertyDescriptors[i];
			if (propertyName.equals(pd.getName())) {
				return pd;
			}
		}
		return null;
	}

	private static void setPropertyAttribute(Class type, String property,
			String attribute, Object value) {
		PropertyDescriptor pd = getPropertyDescriptor(type, property);
		if (pd == null) {
			System.err.println("Warning: property " + property
					+ " is not defined on " + type);
			return;
		}
		pd.setValue(attribute, value);
	}

	private static void setBeanAttribute(Class type, String attribute,
			Object value) {
		getBeanInfo(type).getBeanDescriptor().setValue(attribute, value);
	}

	private static Object getBeanAttribute(Class type, String attribute) {
		return getBeanInfo(type).getBeanDescriptor().getValue(attribute);
	}

	// MetaData registration
	public synchronized static void registerConstructor(String typeName,
			String[] constructor) {
		internalPersistenceDelegates.put(typeName, new DefaultConverter(
				typeName, constructor));
	}

	public synchronized static void registerConstructor(String typeName,
			String[] constructor, Class[] ctr_types) {
		internalPersistenceDelegates.put(typeName, new DefaultConverter(
				typeName, constructor, ctr_types));
	}
	
	public synchronized static void registerFactoryConstructor(Class type, Class fcls, 
			String method, String[] ctrParamNames, Class[] ctrParamTypes){
		DefaultConverter c = new DefaultConverter(type);
		c.setFactoryCtr(fcls, method, ctrParamNames, ctrParamTypes);
		internalPersistenceDelegates.put(type.getName(), c);
	}
	

	public static void removeProperty(String typeName, String property) {
		Vector tp = (Vector) transientProperties.get(typeName);
		if (tp == null) {
			tp = new Vector();
			transientProperties.put(typeName, tp);
		}
		tp.add(property);
	}
}

/*
 * 
 * class ProxyPersistenceDelegate extends DefaultConverter { protected
 * Expression instantiate(Object oldInstance, Encoder out) { Class type =
 * oldInstance.getClass(); java.lang.reflect.Proxy p =
 * (java.lang.reflect.Proxy)oldInstance; // This unappealing hack is not
 * required but makes the // representation of EventHandlers much more concise.
 * java.lang.reflect.InvocationHandler ih =
 * java.lang.reflect.Proxy.getInvocationHandler(p); if (ih instanceof
 * EventHandler) { EventHandler eh = (EventHandler)ih; Vector args = new
 * Vector(); args.add(type.getInterfaces()[0]); args.add(eh.getTarget());
 * args.add(eh.getAction()); if (eh.getEventPropertyName() != null) {
 * args.add(eh.getEventPropertyName()); } if (eh.getListenerMethodName() !=
 * null) { args.setSize(4); args.add(eh.getListenerMethodName()); } return new
 * Expression(oldInstance, EventHandler.class, "create", args.toArray()); }
 * return new Expression(oldInstance, java.lang.reflect.Proxy.class,
 * "newProxyInstance", new Object[]{type.getClassLoader(), type.getInterfaces(),
 * ih}); } }
 */
// Fields
//class java_lang_reflect_Field_PersistenceDelegate extends DefaultConverter {
//	protected static String CLASS = "field_name";
//
//	protected static String NAME = "class";
//
//	public java_lang_reflect_Field_PersistenceDelegate() {
//		super(java.lang.reflect.Field.class);
//	}
//
//	protected Object instantiate(Class type, Map<String, Object> props) {
//		String name = (String) props.get(NAME);
//		Class cls = (Class) props.get(CLASS);
//		try {
//			if (name != null && cls != null)
//				return cls.getField(name);
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		return null;
//	}
//
//	protected static Map<String, Class> map = new HashMap<String, Class>(2);
//	static {
//		map.put(NAME, String.class);
//		map.put(CLASS, Class.class);
//	}
//
//	protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
//	public Map<String, Object> store(Object instance) {
//		Map<String, Object> res = new HashMap<String, Object>(2);
//		res.put(NAME, ((Field) instance).getName());
//		res.put(CLASS, ((Field) instance).getDeclaringClass());
//		return res;
//	}
//}

// Methods
//class java_lang_reflect_Method_PersistenceDelegate extends DefaultConverter {
//	protected static String CLASS = "method_name";
//
//	protected static String NAME = "class";
//
//	public java_lang_reflect_Method_PersistenceDelegate() {
//		super(java.lang.reflect.Method.class);
//	}
//
//	public Object instantiate(Map<String, Object> props) {
//		String name = (String) props.get(NAME);
//		Class cls = (Class) props.get(CLASS);
//		try {
//			if (name != null && cls != null)
//				return cls.getMethod(name);
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		return null;
//	}
//
//	protected static Map<String, Class> map = new HashMap<String, Class>(2);
//	static {
//		map.put(NAME, String.class);
//		map.put(CLASS, Class.class);
//	}
//
//	protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
//	public Map<String, Object> store(Object instance) {
//		Map<String, Object> res = new HashMap<String, Object>(2);
//		res.put(NAME, ((Method) instance).getName());
//		res.put(CLASS, ((Method) instance).getDeclaringClass());
//		return res;
//	}
//}

// AWT
/*
 * class StaticFieldsPersistenceDelegate extends PersistenceDelegate { protected
 * void installFields(Encoder out, Class<?> cls) { Field fields[] =
 * cls.getFields(); for(int i = 0; i < fields.length; i++) { Field field =
 * fields[i]; // Don't install primitives, their identity will not be preserved //
 * by wrapping. if (Object.class.isAssignableFrom(field.getType())) {
 * out.writeExpression(new Expression(field, "get", new Object[]{null})); } } }
 * 
 * protected Expression instantiate(Object oldInstance, Encoder out) { throw new
 * RuntimeException("Unrecognized instance: " + oldInstance); }
 * 
 * public void writeObject(Object oldInstance, Encoder out) { if
 * (out.getAttribute(this) == null) { out.setAttribute(this, Boolean.TRUE);
 * installFields(out, oldInstance.getClass()); } super.writeObject(oldInstance,
 * out); //SystemColor java.awt.font.TextAttribute } }
 */

// SystemColor
// class java_awt_SystemColor_PersistenceDelegate extends
// StaticFieldsPersistenceDelegate {}
// TextAttribute
// class java_awt_font_TextAttribute_PersistenceDelegate extends
// StaticFieldsPersistenceDelegate {}
// Component
class java_awt_Component_PersistenceDelegate extends DefaultConverter {
	private static final String SIZE = "size";

	private static final String LOCATION = "location";
	private static final String BOUNDS = "bounds";
	private static final String FONT = "font";
	private static final String FOREGROUND = "foreground";
	private static final String BACKGROUND = "background";

	private static final Map<String, Class> map = new HashMap<String, Class>(6);
	static {
		map.put(SIZE, Dimension.class);
		map.put(LOCATION, Point.class);
		map.put(BOUNDS, Rectangle.class);
		map.put(FONT, Font.class);
		map.put(FOREGROUND, Color.class);
		map.put(BACKGROUND, Color.class);
	}

	public java_awt_Component_PersistenceDelegate() {
		super(java.awt.Component.class);
	}

	protected void ex_store(Object instance, Map<String, Object> props) {
		java.awt.Component c = (java.awt.Component) instance;
		// The "background", "foreground" and "font" properties.
		// The foreground and font properties of Windows change from
		// null to defined values after the Windows are made visible -
		// special case them for now.
		if (!(instance instanceof java.awt.Window)) {
			props.put(BACKGROUND, c.getBackground());
			props.put(FOREGROUND, c.getForeground());
			props.put(FONT, c.getFont());
		}

		// Bounds
		java.awt.Container p = c.getParent();
		if (p == null || p.getLayout() == null
				&& !(p instanceof javax.swing.JLayeredPane)) {
			props.put(BOUNDS, c.getBounds());
			props.put(LOCATION, c.getLocation());
			props.put(SIZE, c.getSize());
		}
	}

	@Override
	protected void ex_make(Object instance, Map<String, Object> props) {
		java.awt.Component c = (java.awt.Component) instance;

		if (!(instance instanceof java.awt.Window)) {
			if (props.get(BACKGROUND) != null)
				c.setBackground((Color) props.get(BACKGROUND));
			if (props.get(FOREGROUND) != null)
				c.setForeground((Color) props.get(FOREGROUND));
			if (props.get(FONT) != null)
				c.setFont((Font) props.get(FONT));
		}

		// Bounds
		java.awt.Container p = c.getParent();
		if (p == null || p.getLayout() == null
				&& !(p instanceof javax.swing.JLayeredPane)) {
			if (props.get(BOUNDS) != null)
				c.setBounds((Rectangle) props.get(BOUNDS));
			if (props.get(LOCATION) != null)
				c.setLocation((Point) props.get(LOCATION));
			if (props.get(SIZE) != null)
				c.setSize((Dimension) props.get(SIZE));
		}
	}

	@Override
	protected Map<String, Class> getAuxSlots() {
		return map;
	}
}

// Container
class java_awt_Container_PersistenceDelegate extends DefaultConverter {

	public java_awt_Container_PersistenceDelegate() {
		super(Container.class);
	}

	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_COMP,
				new String[]{"component"}, new Class[] { Component.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}

	// protected final String CHILDREN = "components";
	// protected void ex_make(Object instance, Map<String, Object> props) {
	// super.ex_make(instance, props);
	// Ignore the children of a JScrollPane.
	// Pending(milne) find a better way to do this.
	// TODO://
	// if (instance instanceof javax.swing.JScrollPane) {
	// return;
	// }
	// Container oldC = (java.awt.Container) instance;
	// Component[] newChildren = (Component[]) props.get(CHILDREN);
	// if (newChildren != null)
	// for (int i = 0; i < newChildren.length; i++) {
	// oldC.add(newChildren[i]);
	// }
	// }

	// protected void ex_store(Object instance, Map<String, Object> props) {
	// props.put(CHILDREN, ((Container) instance).getComponents());
	// }

	// private static final Map<String, Class> map = new HashMap<String,
	// Class>();
	// static {
	// map.put("components", Component[].class);
	// }

	// protected Map<String, Class> getAuxSlots() {
	// return map;
	// }
}

// Choice
class java_awt_Choice_PersistenceDelegate extends DefaultConverter {

	public java_awt_Choice_PersistenceDelegate() {
		super(Choice.class);
	}

	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_STR,
				new String[]{"name"}, new Class[] { String.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}

	// protected static String ITEMS = "items";

	// protected void ex_store(Object instance, Map<String, Object> props) {
	// Choice c = ((Choice) instance);
	// String[] items = new String[c.getItemCount()];
	// for (int i = 0; i < items.length; i++)
	// items[i] = c.getItem(i);
	// props.put(ITEMS, items);
	// }

	// protected static Map<String, Class> map = new HashMap<String, Class>(2);
	// static {
	// map.put(ITEMS, String[].class);
	// }

	// protected Map<String, Class> getAuxSlots() {
	// return map;
	// }

	// protected void ex_make(Object instance, Map<String, Object> props) {
	// Choice c = ((Choice) instance);
	// String[] items = (String[]) props.get(ITEMS);
	// if (items != null)
	// for (int i = 0; i < items.length; i++)
	// c.add(items[i]);
	// }
}

// Menu
class java_awt_Menu_PersistenceDelegate extends DefaultConverter {
	
	public java_awt_Menu_PersistenceDelegate() {
		super(Menu.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_COMP,
				new String[]{"items"}, new Class[] { Component.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
}

// MenuBar
class java_awt_MenuBar_PersistenceDelegate extends DefaultConverter {
	public java_awt_MenuBar_PersistenceDelegate() {
		super(MenuBar.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_COMP,
				new String[]{"menus"}, new Class[] { Component.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
	/*
	 * protected static String ITEMS = "items";
	 * 
	 * protected void ex_store(Object instance, Map<String, Object> props) {
	 * MenuBar c = ((MenuBar) instance); Menu[] items = new
	 * Menu[c.getMenuCount()]; for (int i = 0; i < items.length; i++) items[i] =
	 * c.getMenu(i); props.put(ITEMS, items); }
	 * 
	 * protected static final Map<String, Class> map = new HashMap<String,
	 * Class>( 2); static { map.put(ITEMS, Menu[].class); }
	 * 
	 * protected Map<String, Class> getAuxSlots() { return map; }
	 * 
	 * protected void ex_make(Object instance, Map<String, Object> props) {
	 * MenuBar c = ((MenuBar) instance); Menu[] items = (Menu[])
	 * props.get(ITEMS); if (items != null) for (int i = 0; i < items.length;
	 * i++) c.add(items[i]); }
	 */
}

// List
class java_awt_List_PersistenceDelegate extends DefaultConverter {
	public java_awt_List_PersistenceDelegate() {
		super(java.awt.List.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_STR,
				new String[]{"items"}, new Class[] { String.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}

	// protected static String ITEMS = "items";

	// protected void ex_store(Object instance, Map<String, Object> props) {
	// java.awt.List c = ((java.awt.List) instance);
	// String[] items = new String[c.getItemCount()];
	// for (int i = 0; i < items.length; i++)
	// items[i] = c.getItem(i);
	// props.put(ITEMS, items);
	// }

	// protected static final Map<String, Class> map = new HashMap<String,
	// Class>(
	// 2);
	// static {
	// map.put(ITEMS, String[].class);
	// }

	// protected Map<String, Class> getAuxSlots() {
	// return map;
	// }

	// protected void ex_make(Object instance, Map<String, Object> props) {
	// java.awt.List c = ((java.awt.List) instance);
	// String[] items = (String[]) props.get(ITEMS);
	// if (items != null)
	// for (int i = 0; i < items.length; i++)
	// c.add(items[i]);
	// }
}

// LayoutManagers

// BorderLayout
class java_awt_BorderLayout_PersistenceDelegate extends DefaultConverter {
	public java_awt_BorderLayout_PersistenceDelegate() {
		super(java.awt.BorderLayout.class);
	}

	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.BORDER_LAYOUT,
				new String[]{ "north", "south", "east", "west", "center" }, null));
	}
	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
}

// CardLayout
class java_awt_CardLayout_PersistenceDelegate extends DefaultConverter {
	public java_awt_CardLayout_PersistenceDelegate() {
		super(java.awt.CardLayout.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.CARD_LAYOUT,
				new String[]{"vector"}, new Class[] { Vector.class }));
	}
	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
}

// GridBagLayout
class java_awt_GridBagLayout_PersistenceDelegate extends DefaultConverter {
	public java_awt_GridBagLayout_PersistenceDelegate() {
		super(java.awt.GridBagLayout.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.GRID_BAG_LAYOUT,
				new String[]{"comptable"}, new Class[] { Hashtable.class }));
	}
	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
	
	protected static String ITEMS = "items";

	protected void ex_store(Object instance, Map<String, Object> props) {
		Hashtable comptable = (Hashtable) RefUtils.getPrivateFieldValue(
				instance.getClass(),
				java.awt.GridBagLayout.class, "comptable");
		props.put(ITEMS, comptable);
	}

	protected static final Map<String, Class> map = new HashMap<String, Class>(
			1);
	static {
		map.put(ITEMS, Hashtable.class);
	}

	protected Map<String, Class> getAuxSlots() {
		return map;
	}

	protected void ex_make(Object instance, Map<String, Object> props) {
		GridBagLayout c = ((GridBagLayout) instance);
		Hashtable comptable = (Hashtable) props.get(ITEMS);
		System.out.println("GBG:" + comptable);
		
		if (comptable != null) {
			for (Enumeration e = comptable.keys(); e.hasMoreElements();) {
				Component child = (Component) e.nextElement();
				System.out.println("GBG - add:" + comptable.get(child) +
						":" + child);
				c.addLayoutComponent(child, comptable.get(child));
			}
		}
	}
}

// Swing

// JFrame (If we do this for Window instead of JFrame, the setVisible call
// will be issued before we have added all the children to the JFrame and
// will appear blank).
/*
 * class javax_swing_JFrame_PersistenceDelegate extends
 * DefaultPersistenceDelegate { protected void initialize(Class<?> type, Object
 * oldInstance, Object newInstance, Encoder out) { super.initialize(type,
 * oldInstance, newInstance, out); java.awt.Window oldC =
 * (java.awt.Window)oldInstance; java.awt.Window newC =
 * (java.awt.Window)newInstance; boolean oldV = oldC.isVisible(); boolean newV =
 * newC.isVisible(); if (newV != oldV) { // false means: don't execute this
 * statement at write time. boolean executeStatements = out.executeStatements;
 * out.executeStatements = false; invokeStatement(oldInstance, "setVisible", new
 * Object[]{Boolean.valueOf(oldV)}, out); out.executeStatements =
 * executeStatements; } } }
 */

// Models
// DefaultListModel
class javax_swing_DefaultListModel_PersistenceDelegate extends DefaultConverter {
	public javax_swing_DefaultListModel_PersistenceDelegate() {
		super(DefaultListModel.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_EL,
				new String[]{"delegate"}, null));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
}

// DefaultComboBoxModel
class javax_swing_DefaultComboBoxModel_PersistenceDelegate extends
		DefaultConverter {
	public javax_swing_DefaultComboBoxModel_PersistenceDelegate() {
		super(DefaultComboBoxModel.class);
	}
	
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_EL,
				new String[]{"objects"}, null));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}

//	protected static String ITEMS = "items";
//	protected void ex_store(Object instance, Map<String, Object> props) {
//		DefaultComboBoxModel m = (DefaultComboBoxModel) instance;
//		Object[] items = new Object[m.getSize()];
//		for (int i = 0; i < m.getSize(); i++)
//			items[i] = m.getElementAt(i);
//		props.put(ITEMS, items);
//	}
//
//	protected static final Map<String, Class> map = new HashMap<String, Class>(
//			1);
//	static {
//		map.put(ITEMS, Object[].class);
//	}
//
//	protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
//	protected void ex_make(Object instance, Map<String, Object> props) {
//		DefaultComboBoxModel c = ((DefaultComboBoxModel) instance);
//		Object[] items = (Object[]) props.get(ITEMS);
//		if (items != null)
//			for (int i = 0; i < items.length; i++)
//				c.addElement(items[i]);
//	}
}

// DefaultMutableTreeNode
class javax_swing_tree_DefaultMutableTreeNode_PersistenceDelegate extends
		DefaultConverter {

	public javax_swing_tree_DefaultMutableTreeNode_PersistenceDelegate() {
		super(DefaultMutableTreeNode.class);
	}
	
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_TREE,
				new String[]{"children"}, null));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}

//	protected static String ITEMS = "items";
//
//	protected void ex_store(Object instance, Map<String, Object> props) {
//		DefaultMutableTreeNode m = (DefaultMutableTreeNode) instance;
//		TreeNode[] items = new TreeNode[m.getChildCount()];
//		for (int i = 0; i < m.getChildCount(); i++)
//			items[i] = m.getChildAt(i);
//		props.put(ITEMS, items);
//	}
//
//	protected static final Map<String, Class> map = new HashMap<String, Class>(
//			1);
//	static {
//		map.put(ITEMS, TreeNode[].class);
//	}
//
//	protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
//	protected void ex_make(Object instance, Map<String, Object> props) {
//		DefaultMutableTreeNode c = ((DefaultMutableTreeNode) instance);
//		TreeNode[] items = (TreeNode[]) props.get(ITEMS);
//		if (items != null)
//			for (int i = 0; i < items.length; i++)
//				c.add((MutableTreeNode) items[i]);
//	}
}


// JTabbedPane
class javax_swing_JTabbedPane_PersistenceDelegate extends DefaultConverter {

	public javax_swing_JTabbedPane_PersistenceDelegate() {
		super(JTabbedPane.class);
	}
	
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_TAB,
				new String[]{"componentAt", "titleAt", "iconAt"}, 
				new Class[] { Component.class, String.class, Icon.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}

//	
//	protected static String ITEMS = "items";
//	protected static String TITLES = "titles";
//	protected static String ICONS = "icons";
//
//	protected void ex_store(Object instance, Map<String, Object> props) {
//		JTabbedPane m = (JTabbedPane) instance;
//		Component[] items = new Component[m.getTabCount()];
//		String[] titles = new String[m.getTabCount()];
//		Icon[] icons = new Icon[m.getTabCount()];
//		for (int i = 0; i < m.getTabCount(); i++) {
//			items[i] = m.getComponentAt(i);
//			titles[i] = m.getTitleAt(i);
//			icons[i] = m.getIconAt(i);
//		}
//		props.put(ITEMS, items);
//		props.put(TITLES, titles);
//		props.put(ICONS, icons);
//	}
//
//	protected static final Map<String, Class> map = new HashMap<String, Class>(
//			3);
//	static {
//		map.put(ITEMS, Component[].class); 
//		map.put(TITLES, String[].class);
//		map.put(ICONS, Icon[].class);
//	}
//
//	protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
//	protected void ex_make(Object instance, Map<String, Object> props) {
//		JTabbedPane c = ((JTabbedPane) instance);
//		Component[] items = (Component[]) props.get(ITEMS);
//		String[] titles = (String[]) props.get(TITLES);
//		Icon[] icons = (Icon[]) props.get(ICONS);
//		if (items != null && titles != null && icons != null)
//			for (int i = 0; i < items.length; i++)
//				c.addTab(titles[i], icons[i], items[i]);
//	}
}

// JMenu
// Note that we do not need to state the initialiser for
// JMenuItems since the getComponents() method defined in
// Container will return all of the sub menu items that
// need to be added to the menu item.
// Not so for JMenu apparently.
class javax_swing_JMenu_PersistenceDelegate extends DefaultConverter {
	public javax_swing_JMenu_PersistenceDelegate() {
		super(JMenu.class);
	}
	
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_COMP,
				new String[]{"menuComponents"}, new Class[] { Component.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
//	protected static String ITEMS = "items";
//
//	protected void ex_store(Object instance, Map<String, Object> props) {
//		JMenu c = ((JMenu) instance);
//		ArrayList items = new ArrayList();
//		// System.out.println("JMenu -ex_store: " + c.getItemCount());
//		for (int i = 0; i < c.getItemCount(); i++) {
//			// System.out.println("JMenu -ex_store - item: " + c.getItem(i));
//			items.add(c.getItem(i));
//		}
//		props.put(ITEMS, items);
//	}

	//protected static Map<String, Class> map = new HashMap<String, Class>(1);
	//static {
//		map.put(ITEMS, ArrayList.class);
	//}

	//protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
//	protected void ex_make(Object instance, Map<String, Object> props) {
//		JMenu c = ((JMenu) instance);
//		ArrayList items = (ArrayList) props.get(ITEMS);
//		if (items != null)
//			for (int i = 0; i < items.size(); i++) {
//				if (items.get(i) != null) {
//					c.add((JMenuItem) items.get(i));
//					// if this was undefined during storing, negative values
//					// could creep in
//					// which leads to strange sizing
//					Dimension dim = ((JMenuItem) items.get(i))
//							.getPreferredSize();
//					if (dim.height < 0 || dim.width < 0)
//						((JMenuItem) items.get(i)).setPreferredSize(null);
//				} else
//					c.addSeparator();
//			}
//	}

}

class javax_swing_JMenuBar_PersistenceDelegate extends DefaultConverter {
	
	public javax_swing_JMenuBar_PersistenceDelegate() {
		super(JMenuBar.class);
	}
	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
	static {
		addOnFields.add(new Converter.Add(AddOnFactory.ADD_COMP,
				new String[]{"component"}, new Class[] { Component.class }));
	}

	public Set<AddOnType> getAddOnFields() {
		return addOnFields;
	}
	

	/*
	 * protected static String ITEMS = "items";
	 * protected void ex_store(Object instance, Map<String, Object> props) {
	 * JMenuBar c = ((JMenuBar) instance); JMenu[] items = new
	 * JMenu[c.getMenuCount()]; for (int i = 0; i < items.length; i++) items[i] =
	 * c.getMenu(i); props.put(ITEMS, items); //System.out.println("JMenuBar
	 * -ex_store: " + items.length); }
	 * 
	 * static final Map<String, Class> map = new HashMap<String, Class>(1);
	 * static { map.put(ITEMS, JMenu[].class); }
	 * 
	 * protected Map<String, Class> getAuxSlots() { return map; }
	 * 
	 * protected void ex_make(Object instance, Map<String, Object> props) {
	 * JMenuBar c = ((JMenuBar) instance); //System.out.println("JMenuBar
	 * -ex_make: " + c.getMenuCount()); JMenu[] items = (JMenu[])
	 * props.get(ITEMS); //System.out.println("JMenuBar -ex_make: " +
	 * items.length); if (items != null) for (int i = 0; i < items.length; i++) {
	 * if (items[i] != null) { c.add(items[i]); } }
	 * //System.out.println("JMenuBar -ex_make: " + c.getMenuCount()); }
	 */

}

//class javax_swing_AbstractAction_PersistenceDelegate extends DefaultConverter {
//	public javax_swing_AbstractAction_PersistenceDelegate() {
//		super(AbstractAction.class);
//	}
//
//	private static final Set<AddOnType> addOnFields = new HashSet<AddOnType>();
//	static {
//		addOnFields.add(new Converter.Add(AddOnFactory.ACTION,
//				new String[]{"PROPS"}, new Class[]{Map.class}));
//	}
//
//	public Set<AddOnType> getAddOnFields() {
//		return addOnFields;
//	}
//	protected static String PROPS = "props";
//
//	static final String[] KEYS = new String[] { Action.NAME,
//			Action.SHORT_DESCRIPTION, Action.LONG_DESCRIPTION,
//			Action.SMALL_ICON, Action.ACTION_COMMAND_KEY,
//			Action.ACCELERATOR_KEY, Action.MNEMONIC_KEY };
//
//	protected void ex_store(Object instance, Map<String, Object> props) {
//		Action a = (Action) instance;
//		Map map = new HashMap();
//		for (String key : KEYS)
//			if (a.getValue(key) != null)
//				map.put(key, a.getValue(key));
//		props.put(PROPS, map);
//	}
//
//	protected static Map<String, Class> map = new HashMap<String, Class>(1);
//	static {
//		map.put(PROPS, Map.class);
//	}
//
//	protected Map<String, Class> getAuxSlots() {
//		return map;
//	}
//
////	
////	protected void ex_make(Object instance, Map<String, Object> props) {
////		Action a = (Action) instance;
////		Map map = (Map) props.get(PROPS);
////		if (map != null) {
////			// System.out.println("ActionCOnverter - make:" + map + ":"
////			// + map.size());
////			for (Object key : map.keySet())
////				a.putValue((String) key, map.get(key));
////		}
////	}
//
//}

//class javax_swing_ImageIcon_PersistenceDelegate implements Converter {
//
//	protected static String IMAGE = "image";
//
//	protected static Map<String, Class> map = new HashMap<String, Class>(1);
//	static {
//		map.put(IMAGE, byte[].class);
//	}
//
//	public Map<String, Class> getSlots() {
//		return map;
//	}
//
//	public Object make(Map<String, Object> props) {
//		byte[] b = (byte[]) props.get(IMAGE);
//		try {
//			// System.out.println("ImageIcon instantiate");
//			ByteArrayInputStream in = new ByteArrayInputStream(b);
//			ObjectInputStream objectIn = new ObjectInputStream(in);
//			return (ImageIcon) objectIn.readObject();
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		return null;
//	}
//
//	public Map<String, Object> store(Object instance) {
//		try {
//			Map<String, Object> props = new HashMap<String, Object>(1);
//			ByteArrayOutputStream out = new ByteArrayOutputStream();
//			ObjectOutputStream objectOut = new ObjectOutputStream(out);
//			objectOut.writeObject(instance);
//			objectOut.flush();
//			props.put(IMAGE, out.toByteArray());
//			return props;
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		}
//		return null;
//	}
//}
class javax_swing_JScrollBar_PersistenceDelegate extends DefaultConverter {
	
	public javax_swing_JScrollBar_PersistenceDelegate() {
		super(JScrollBar.class);
	}
	
	public Set<AddOnType> getAllAddOnFields() {
		return null;
	}
}

class javax_swing_JScrollPane_PersistenceDelegate extends DefaultConverter {
	
	public javax_swing_JScrollPane_PersistenceDelegate() {
		super(JScrollPane.class);
	}
	
	public Set<AddOnType> getAllAddOnFields() {
		return null;
	}
}
class javax_swing_JFrame_PersistenceDelegate extends DefaultConverter {
	
	public javax_swing_JFrame_PersistenceDelegate() {
		super(JFrame.class);
	}
	
	public Set<AddOnType> getAllAddOnFields() {
		return null;
	}
}

class javax_swing_JPanel_PersistenceDelegate extends DefaultConverter {
	
	public javax_swing_JPanel_PersistenceDelegate() {
		super(JPanel.class);
	}
	
	public Set<AddOnType> getAllAddOnFields() {
		return null;
	}
}

