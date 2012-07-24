package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tkym.labs.record.TableMeta.ColumnMeta;


import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.tkym.labs.record.RecordstoreExecutor;
import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementException;


public class EntityConverter001Test {
	private static TableMeta one = TableMeta.table("one").key("id").asLong()
			.column("name").asString().meta();

	private static TableMeta child = TableMeta.table("child").key("parent")
			.asLong().key("id").asLong().column("name").asString().meta();

	private static TableMeta grandchild = TableMeta.table("grandchild")
			.key("grandparent").asLong().key("parent").asLong().key("id")
			.asLong().column("name").asString().meta();

	Key<Long> one(long id) {
		return new Key<Long>("one", Long.class, id);
	}

	Key<Long> child(Key<?> parent, long id) {
		return new Key<Long>("child", parent, Long.class, id);
	}

	Key<Long> grandchild(Key<?> parent, long id) {
		return new Key<Long>("grandchild", parent, Long.class, id);
	}

	@Test
	public void testConstructorCase001() {
		new KeyConverter(one);
	}

	@Test
	public void testConstructorCase002() {
		new KeyConverter(one, child);
	}

	@Test
	public void testConstructorCase003() {
		new KeyConverter(one, child, grandchild);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructor_ErrorCase001() {
		new KeyConverter(child, one);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructor_ErrorCase002() {
		new KeyConverter(one, one);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructor_ErrorCase003() {
		new KeyConverter(child, child);
	}

	@Test
	public void testToPrimaryKey_Case001() {
		KeyConverter converter = new KeyConverter(one);
		RecordKey key = converter.convertToPrimaryKeyFrom(one(1L));
		assertThat(key.getTableMeta(), is(one));
		assertThat(key.getValues().length, is(1));
		assertThat((Long) key.getValues()[0], is(1L));
	}

	@Test
	public void testToPrimaryKey_Case002() {
		KeyConverter converter = new KeyConverter(one, child);
		RecordKey key = converter.convertToPrimaryKeyFrom(child(one(1L), 2L));
		assertThat(key.getTableMeta(), is(child));
		assertThat(key.getValues().length, is(2));
		assertThat((Long) key.getValues()[0], is(1L));
		assertThat((Long) key.getValues()[1], is(2L));
	}
	
	@Test
	public void testToPrimaryKey_Case003() {
		KeyConverter converter = new KeyConverter(one, child,
				grandchild);
		RecordKey key = converter.convertToPrimaryKeyFrom(grandchild(child(one(1L), 2L),
				3L));
		assertThat(key.getTableMeta(), is(grandchild));
		assertThat(key.getValues().length, is(3));
		assertThat((Long) key.getValues()[0], is(1L));
		assertThat((Long) key.getValues()[1], is(2L));
		assertThat((Long) key.getValues()[2], is(3L));
	}

	@Test
	public void testHierarchyKey_Case001() {
		KeyConverter converter = new KeyConverter(one);
		@SuppressWarnings("unchecked")
		Key<Long> key = (Key<Long>)converter.convertToKeyFrom(new RecordKey(one, 1L));
		assertThat(key.getParent(), nullValue());
		assertThat(key.getValue(), is(1L));
		assertThat(key.getKind(), is(one.tableName()));
	}
	
	@Test
	public void testHierarchyKey_Case002() {
		KeyConverter converter = new KeyConverter(one, child);
		@SuppressWarnings("unchecked")
		Key<Long> childKey = (Key<Long>)converter.convertToKeyFrom(new RecordKey(child, 1L, 2L));
		assertThat(childKey.getParent(), CoreMatchers.notNullValue());
		assertThat(childKey.getValue(), is(2L));
		assertThat(childKey.getKind(), is(child.tableName()));
		@SuppressWarnings("unchecked")
		Key<Long> oneKey = (Key<Long>)childKey.getParent();
		assertThat(oneKey.getParent(), nullValue());
		assertThat(oneKey.getValue(), is(1L));
		assertThat(oneKey.getKind(), is(one.tableName()));
	}

	@Test
	public void testHierarchyKey_Case003() {
		KeyConverter converter = new KeyConverter(one, child, grandchild);
		@SuppressWarnings("unchecked")
		Key<Long> grandchildKey = (Key<Long>)converter.convertToKeyFrom(new RecordKey(grandchild, 1L, 2L, 3L));
		assertThat(grandchildKey.getParent(), CoreMatchers.notNullValue());
		assertThat(grandchildKey.getValue(), is(3L));
		assertThat(grandchildKey.getKind(), is(grandchild.tableName()));
		@SuppressWarnings("unchecked")
		Key<Long> childKey = (Key<Long>)grandchildKey.getParent();
		assertThat(childKey.getParent(), CoreMatchers.notNullValue());
		assertThat(childKey.getValue(), is(2L));
		assertThat(childKey.getKind(), is(child.tableName()));
		@SuppressWarnings("unchecked")
		Key<Long> oneKey = (Key<Long>)childKey.getParent();
		assertThat(oneKey.getParent(), nullValue());
		assertThat(oneKey.getValue(), is(1L));
		assertThat(oneKey.getKind(), is(one.tableName()));
	}
	
	class Datastore<K>{
		private final RecordstoreExecutor executor;
		private KeyConverter converter;
		
		Datastore(RecordstoreExecutor executor, TableMeta... tableMetas){
			this.executor = executor;
			if(tableMetas == null || tableMetas.length == 0)
				throw new IllegalArgumentException("tableMeta is not exists");
			converter = new KeyConverter(tableMetas);
		}
		
		public void delete(Key<K> key) throws RecordstoreException{
			try {
				executor.delete(converter.convertToPrimaryKeyFrom(key));
			} catch (SQLException e) {
				throw new RecordstoreException(e);
			} catch (PreparedStatementException e) {
				throw new RecordstoreException(e);
			}
		}
		
		public void put(Entity<K> entity) throws RecordstoreException{
			Record record = converter.convertToRecordFrom(entity);
			try {
				if(executor.exists(record.key()))
					executor.update(record);
				else
					executor.insert(record);
			} catch (Exception e) {
				throw new RecordstoreException(e);
			}
		}
		
		public boolean contains(Key<K> key) throws RecordstoreException{
			return executor.exists(converter.convertToPrimaryKeyFrom(key));
		}
	}
	
	public class Entity<K>{
		private final Key<K> key;
		private final Map<String, Object> values;
		Entity(Key<K> key, Map<String, Object> values){
			this.key = key;
			this.values = values;
		}
		public Entity(Key<K> key){
			this(key, new HashMap<String, Object>());
		}
		public Object put(String property, Object value){
			return values.put(property, value);
		}
		public Object get(String property){
			return values.get(property);
		}
		public Key<K> getKey() {
			return key;
		}
	}
	
	class KeyConverter {
		private TableMeta tableMeta;
		private KeyConvertInfo<?>[] infoArray;
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		KeyConverter(TableMeta... tableMetas) {
			infoArray = new KeyConvertInfo[tableMetas.length];
			for (int i = 0; i < tableMetas.length; i++) {
				TableMeta meta = tableMetas[i];
				ensureKeySize(i + 1, meta);
				ColumnMeta keyMeta = meta.keys()[i];
				infoArray[i] = new KeyConvertInfo(meta.tableName(), keyMeta.getName(), keyMeta.getType().getClassType());
			}
			tableMeta = tableMetas[tableMetas.length - 1];
		}
		
		private void ensureKeySize(int level, TableMeta meta) {
			int keySizeOfTableMeta = meta.keys().length;
			if (keySizeOfTableMeta != level)
				throw new IllegalArgumentException(
						"keySize of TableMeta is invalid: keySize="
								+ keySizeOfTableMeta + " level=" + level);
		}

		Key<?> convertToKeyFrom(RecordKey primaryKey) {
			Key<?> parent = null;
			for (int i = 0; i < infoArray.length; i++)
				parent = createKeyUncheck(parent, infoArray[i], primaryKey.getValues()[i]);
			return parent;
		}
		
		Entity<?> convertToEntityFrom(Record record){
			return createEntityUncheck(convertToKeyFrom(record.key()), record.asMap()); 
		}
		
		private Entity<?> createEntityUncheck(Key<?> key, Map<String, Object> map){
			return createEntity(key, map);
		}
		
		private <T> Entity<T> createEntity(Key<T> key, Map<String, Object> map){
			return new Entity<T>(key, map);
		}
		
		@SuppressWarnings({ "unchecked" })
		private Key<?> createKeyUncheck(Key<?> parent, KeyConvertInfo<?> info, Object value){
			return createKey(parent, (KeyConvertInfo<Object>)info, (Object)value);
		}
		
		private <T> Key<T> createKey(Key<?> parent, KeyConvertInfo<T> info, T value){
			return new Key<T>(info.getKind(), parent, info.getType(), value);
		}
		
		RecordKey convertToPrimaryKeyFrom(Key<?> key) {
			List<?> values = getValuesByRecurrence(key);
			Object[] array = new Object[values.size()];
			values.toArray(array);
			return new RecordKey(tableMeta, array);
		}
		
		Record convertToRecordFrom(Entity<?> entity){
			RecordKey primaryKey = convertToPrimaryKeyFrom(entity.getKey());
			return new Record(primaryKey, entity.values);
		}
		
		private List<?> getValuesByRecurrence(Key<?> key) {
			List<Object> values = new ArrayList<Object>();
			if (key.getParent() != null)
				values.addAll(getValuesByRecurrence(key.getParent()));
			values.add(key.getValue());
			return values;
		}
	}
	
	class KeyConvertInfo<T>{
		private final String kind;
		private final String name;
		private final Class<T> type;
		public KeyConvertInfo(String kind, String name, Class<T> type) {
			this.kind = kind;
			this.name = name;
			this.type = type;
		}
		public String getKind() {
			return kind;
		}
		public String getName() {
			return name;
		}
		public Class<T> getType() {
			return type;
		}
	}
	
	class Key<T> {
		private final String kind;
		private final Key<?> parent;
		private final T value;
		private final Class<T> type;
		Key(String kind, Class<T> type, T value) {
			this(kind, null, type, value);
		}
		Key(String kind, Key<?> parent, Class<T> type, T value) {
			this.kind = kind;
			this.parent = parent;
			this.type = type;
			this.value = value;
		}
		public String getKind() {
			return kind;
		}
		public Key<?> getParent() {
			return parent;
		}
		public T getValue() {
			return value;
		}
		public Class<T> getType() {
			return type;
		}
	}
}
