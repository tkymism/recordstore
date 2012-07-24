package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tkym.labs.record.QueryFilter.QueryFilterOperator;
import com.tkym.labs.record.TableMeta.ColumnMeta;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.DDLExecutor;
import com.tkym.labs.record.RecordFetcherImpl;
import com.tkym.labs.record.RecordstoreExecutor;
import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementException;


public class RecordstoreExecutorTest {
	private static Connection connection;
	/**
	 * Test Data: User
	 */
	private static TableMeta userMeta = 
			TableMeta
				.table("user")
				.key("userId").type(ColumnMeta.LONG)
				.column("firstName").type(ColumnMeta.STRING)
				.column("familyName").type(ColumnMeta.STRING)
				.column("birth").type(ColumnMeta.INTEGER)
				.meta();
	
	/**
	 * Test Data: Account
	 */
	private static TableMeta accountMeta = 
			TableMeta
				.table("account")
				.key("userId").type(ColumnMeta.LONG)
				.key("email").type(ColumnMeta.STRING)
				.column("profile1").type(ColumnMeta.STRING)
				.column("profile2").type(ColumnMeta.STRING)
				.column("profile3").type(ColumnMeta.STRING)
				.meta();
	
	/**
	 * Factory Method of Test Data :user 
	 * @param id
	 * @return
	 */
	private static Record user(long id){
		RecordKey key = new RecordKey(userMeta, id);
		Record entity = new Record(key);
		entity.put("firstName", "");
		entity.put("familyName", "");
		entity.put("birth", 0);
		return entity;
	}
	
	/**
	 * Factory Method of Test Data :account
	 * @param id
	 * @param email
	 * @return
	 */
	private static Record account(long id, String email){
		RecordKey key = new RecordKey(accountMeta, id, email);
		Record entity = new Record(key);
		entity.put("profile1", "profile1 of "+email);
		entity.put("profile2", "profile2 of "+email);
		entity.put("profile3", "profile3 of "+email);
		return entity;
	}
	
	@BeforeClass
	public static void setupClass() throws SQLException, ClassNotFoundException{
		connection = new SqliteFactory().create(SqliteConnectionTest.class.getResource("test.db"));
		connection.setAutoCommit(false);
		DDLExecutor executor = SqliteFactory.ddl(connection);
		
		try {
			executor.drop(userMeta);
		} catch (SQLException e) {}
		
		try {
			executor.drop(accountMeta);
		} catch (SQLException e) {}
		
		executor.create(userMeta);
		executor.create(accountMeta);
	}
	
	@AfterClass
	public static void teardownClass() throws SQLException, ClassNotFoundException{
		DDLExecutor executor = SqliteFactory.ddl(connection);
		executor.drop(userMeta);
		executor.drop(accountMeta);
		connection.close();
	}
	
	@Test
	public void testNormalTest() throws SQLException, PreparedStatementException{
		RecordstoreExecutor executor = SqliteFactory.executor(connection);
		
		assertThat(executor.insert(user((long)0)), is(1));
		assertThat(executor.insert(account((long)0, 0+"@email.com")), is(1));
		
		Record user = user((long)0);
		Record account = account((long)0, 0+"@email.com");
		user.put("firstName", "foo");
		user.put("familyName", "bar");
		account.put("profile1", "hoge");
		
		assertThat(executor.update(user), is(1));
		assertThat(executor.update(account), is(1));
		
		assertThat(executor.delete(user((long)0).key()), is(1));
		assertThat(executor.delete(account((long)0, 0+"@email.com").key()), is(1));
	}
	
	@Test
	public void testQueryCase001() throws SQLException, RecordstoreException, PreparedStatementException{
		RecordstoreExecutor executor = SqliteFactory.executor(connection);
		List<Record> records = new ArrayList<Record>();
		for(long i=0; i<1000; i++) records.add(user(i));
		for(Record record : records)
			assertThat(executor.insert(record), is(1));
		QueryFilterCriteria filter = new QueryFilter<Long>("userId", QueryFilterOperator.EQUAL, 0L);
		RecordFetcherImpl fetcher = executor.executeQuery(userMeta, filter, null, false);
		assertThat(fetcher.next(), is(true));
		Record user = fetcher.getRecord();
		assertThat((Long)user.key().getValues()[0], is(0L));
		assertThat(fetcher.next(), is(false));
		for(Record record : records)
			assertThat(executor.delete(record.key()), is(1));
	}
	
	@Test
	public void testQueryCase003() throws SQLException, RecordstoreException, PreparedStatementException{
		RecordstoreExecutor executor = SqliteFactory.executor(connection);
		List<Record> records = new ArrayList<Record>();
		for(long i=0; i<1000; i++) records.add(account(i/10, i+"@email.com"));
		for(Record record : records)
			assertThat(executor.insert(record), is(1));
		
		QueryFilterCriteria filter = new QueryFilter<Long>("userId", QueryFilterOperator.EQUAL, 1L);
		RecordFetcherImpl fetcher = executor.executeQuery(accountMeta, filter, null, false);
		while(fetcher.next()) fetcher.getRecord();
		
		for(Record record : records)
			assertThat(executor.delete(record.key()), is(1));
	}
	
	@Test
	public void testQueryBuilderCase_asList() throws RecordstoreException, SQLException, PreparedStatementException{
		RecordstoreExecutor executor = SqliteFactory.executor(connection);
		List<Record> records = new ArrayList<Record>();
		for(long i=0; i<1000; i++) records.add(user(i));
		for(Record record : records)
			assertThat(executor.insert(record), is(1));
		List<Record> list = executor
				.query(userMeta)
				.filter("userId", Long.class).equalsTo(3L)
				.read(QueryFetcher.RECORD)
				.asList();
		assertThat((Long)list.get(0).key().getValues()[0], is(3L));
		for(Record record : records)
			assertThat(executor.delete(record.key()), is(1));
	}
}