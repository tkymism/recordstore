package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.RecordstoreExecutor;
import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementException;

public class RecordstoreExecutorIntegrationTest {
	private static RecordstoreExecutor executor;
	private static Connection connection;
	
	/**
	 * Test Data: User
	 */
	private static TableMeta accountMeta = 
			TableMeta
				.table("account")
				.key("id").asLong()
				.key("email").asString()
				.column("firstName").asString()
				.column("familyName").asString()
				.column("age").asInteger()
				.meta();
	
	/**
	 * Factory Method of Test Data :user 
	 * @param id
	 * @return
	 */
	private static Record account(long id, String email){
		RecordKey key = new RecordKey(accountMeta, id, email);
		Record entity = new Record(key);
		entity.put("firstName", "first"+id%10);
		entity.put("familyName","family"+id%5);
		entity.put("age", ((int)id%10)+20);
		return entity;
	}
	
	private static List<Record> accounts(){
		List<Record> list = new ArrayList<Record>();
		for(long i=0; i<1000; i++)
			list.add(account(i, "email"+i+"@hoge.com"));
		return list;
	}
	
	@BeforeClass
	public static void setupClass() throws SQLException, ClassNotFoundException, PreparedStatementException{
		connection = new SqliteFactory().create(SqliteConnectionTest.class.getResource("test.db"));
		connection.setAutoCommit(false);
		executor = SqliteFactory.executor(connection);
		
		try {
			executor.drop(accountMeta);
		} catch (SQLException e) {
		}
		executor.create(accountMeta);
		for(Record r : accounts()) executor.insert(r);
		connection.commit();
	}
	
	@AfterClass
	public static void teardownClass() throws SQLException, ClassNotFoundException{
		executor.drop(accountMeta);
		connection.close();
	}

	@Test
	public void testQueryBuilderCase001() throws RecordstoreException{
		List<RecordKey> keyList = 
				executor.query(accountMeta)
				.filter("firstName").startsWith("first1")
				.sort("id").asc()
				.read(QueryFetcher.PRIMARY_KEY)
				.asList();
		assertThat(keyList.size(), is(100));
	}

	@Test
	public void testQueryBuilderCase002() throws RecordstoreException{
		List<RecordKey> keyList = 
				executor.query(accountMeta)
				.filter("age").greaterEqual(21)
				.filter("age").lessEqual(22)
				.sort("id").asc()
				.read(QueryFetcher.PRIMARY_KEY)
				.asList();
		assertThat(keyList.size(), is(200));
	}

	@Test
	public void testQueryBuilderCase003() throws RecordstoreException{
		List<Record> list = 
				executor.query(accountMeta)
				.filter("age").greaterThan(20)
				.filter("age").lessThan(22)
				.sort("id").asc()
				.read(QueryFetcher.RECORD)
				.asList();
		
		assertThat(list.size(), is(100));
		for(Record r : list)
			assertThat(r.get("age", int.class), is(21));
	}

	@Test
	public void testQueryBuilderCase004() throws RecordstoreException{
		List<Record> list = 
				executor.query(accountMeta)
				.filter("age").greaterEqual(21)
				.filter("age").lessEqual(21)
				.sort("id").asc()
				.read(QueryFetcher.RECORD)
				.asList();
		assertThat(list.size(), is(100));
		for(Record r : list)
			assertThat(r.get("age", int.class), is(21));
	}
}