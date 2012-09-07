package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.List;

import com.tkym.labs.record.TableMeta.ColumnMeta;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.DDLExecutor;
import com.tkym.labs.record.RecordstoreServiceImpl;


public class RecordstoreServiceTest {
	private static RecordstoreServiceFactory factory; 
	private static Connection connection;
	
	@BeforeClass
	public static void setupClass() throws SQLException, ClassNotFoundException, StatementExecuteException{
		connection = new SqliteFactory().create(SqliteConnectionTest.class.getResource("test.db"));
		connection.setAutoCommit(false);
		factory = new RecordstoreServiceFactory() {
			@Override
			public RecordstoreServiceImpl create() {
				return new RecordstoreServiceImpl(connection, new SqliteDialect());
			}
		};
		
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
	public static void teardownClass() throws SQLException, ClassNotFoundException, StatementExecuteException{
		DDLExecutor executor = SqliteFactory.ddl(connection);
		executor.drop(userMeta);
		executor.drop(accountMeta);
		connection.close();
	}
	
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
	
	private static DecimalFormat formatter = new DecimalFormat("000");
	
	/**
	 * Factory Method of Test Data :user 
	 * @param id
	 * @return
	 */
	private static Record user(long id){
		RecordKey key = new RecordKey(userMeta, id);
		Record entity = new Record(key);
		entity.put("firstName", "foo"+formatter.format(id));
		entity.put("familyName", "bar"+formatter.format(id));
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
	
	@Test
	public void testQueryCase001() throws RecordstoreException{
		RecordstoreService service = factory.create();
		for(int i=0; i<100; i++)
			service.put(user((long)i));
		for(int i=0; i<100; i++)
			for(int j=0; j<20; j++)
				service.put(account((long)i,"email"+j));
		List<RecordKey> list = 
				service.query(userMeta).
				key().asList();
		assertThat(list.size(), is(100));
	}

	@Test
	public void testQueryCase002() throws RecordstoreException{
		RecordstoreService service = factory.create();
		for(int i=0; i<1000; i++)
			service.put(user((long)i));
		List<RecordKey> list = 
				service.query(userMeta).
				filter("firstName").greaterEqual("foo5").
				filter("firstName").lessEqual("foo6").
				key().asList();
		for(RecordKey key : list)
			Assert.assertTrue(((String)service.get(key).get("firstName")).startsWith("foo5"));
	}
}
