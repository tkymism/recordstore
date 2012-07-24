
package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.tkym.labs.record.TableMeta.ColumnMeta;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.DDLExecutor;
import com.tkym.labs.record.PreparedStatementProvider;
import com.tkym.labs.record.RecordstoreBindHelper;
import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementException;
import com.tkym.labs.record.PreparedStatementProvider.PreparedStatementType;


/**
 * @author takayama
 */
public class PreparedStatementProviderTest {
	private static Connection connection;
	private static TableMeta user = 
			TableMeta
				.table("user")
				.key("userId").type(ColumnMeta.LONG)
				.column("firstName").type(ColumnMeta.STRING)
				.column("familyName").type(ColumnMeta.STRING)
				.column("birth").type(ColumnMeta.INTEGER)
				.meta();
	
	private static TableMeta account = 
			TableMeta
				.table("account")
				.key("userId").type(ColumnMeta.LONG)
				.key("email").type(ColumnMeta.STRING)
				.column("profile1").type(ColumnMeta.STRING)
				.column("profile2").type(ColumnMeta.STRING)
				.column("profile3").type(ColumnMeta.STRING)
				.meta();
	
	@BeforeClass
	public static void setupClass() throws SQLException, ClassNotFoundException{
		connection = new SqliteFactory().create(SqliteConnectionTest.class.getResource("test.db"));
		DDLExecutor executor = SqliteFactory.ddl(connection);
		try {
			executor.drop(user);
		} catch (SQLException e) {}
		try {
			executor.drop(account);
		} catch (SQLException e) {}
		executor.create(user);
		executor.create(account);
	}
	
	@AfterClass
	public static void teardownClass() throws SQLException, ClassNotFoundException{
		DDLExecutor executor = SqliteFactory.ddl(connection);
		executor.drop(user);
		executor.drop(account);
		connection.close();
	}

	@Test
	public void testUserCase001() throws SQLException, PreparedStatementException{
		PreparedStatement preparedStatement;
		int ret;
		RecordstoreBindHelper.PreparedStatementBinder helper;
		PreparedStatementProvider provider = new PreparedStatementProvider(connection);
		preparedStatement = provider.get(user, PreparedStatementType.INSERT);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user001", ColumnMeta.STRING);
		helper.set("sato", ColumnMeta.STRING);
		helper.set("taro", ColumnMeta.STRING);
		helper.set(20110512, ColumnMeta.INTEGER);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.UPDATE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("suzuki", ColumnMeta.STRING);
		helper.set("taro", ColumnMeta.STRING);
		helper.set(20110512, ColumnMeta.INTEGER);
		helper.set("user001", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.DELETE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user001", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
	}
	
	@Test
	public void testAccountCase001() throws SQLException, PreparedStatementException{
		PreparedStatement preparedStatement;
		int ret;
		RecordstoreBindHelper.PreparedStatementBinder helper;
		PreparedStatementProvider provider = new PreparedStatementProvider(connection);
		
		preparedStatement = provider.get(account, PreparedStatementType.INSERT);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user001", ColumnMeta.STRING);
		helper.set("user001@company.com", ColumnMeta.STRING);
		helper.set("aaa", ColumnMeta.STRING);
		helper.set("bbb", ColumnMeta.STRING);
		helper.set("ccc", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(account, PreparedStatementType.UPDATE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("aaa_aaa", ColumnMeta.STRING);
		helper.set("bbb_bbb", ColumnMeta.STRING);
		helper.set("ccc_ccc", ColumnMeta.STRING);
		helper.set("user001", ColumnMeta.STRING);
		helper.set("user001@company.com", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(account, PreparedStatementType.DELETE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user001", ColumnMeta.STRING);
		helper.set("user001@company.com", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
	}

	@Test
	public void testUserCase002() throws SQLException, PreparedStatementException{
		PreparedStatement preparedStatement;
		int ret;
		RecordstoreBindHelper.PreparedStatementBinder helper;
		PreparedStatementProvider provider = new PreparedStatementProvider(connection);
		preparedStatement = provider.get(user, PreparedStatementType.INSERT);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user001", ColumnMeta.STRING);
		helper.set("sato", ColumnMeta.STRING);
		helper.set("taro", ColumnMeta.STRING);
		helper.set(20110512, ColumnMeta.INTEGER);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.INSERT);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user002", ColumnMeta.STRING);
		helper.set("sato", ColumnMeta.STRING);
		helper.set("hanako", ColumnMeta.STRING);
		helper.set(20110512, ColumnMeta.INTEGER);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.UPDATE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("suzuki", ColumnMeta.STRING);
		helper.set("taro", ColumnMeta.STRING);
		helper.set(20110512, ColumnMeta.INTEGER);
		helper.set("user001", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.UPDATE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("suzuki", ColumnMeta.STRING);
		helper.set("hanako", ColumnMeta.STRING);
		helper.set(20110512, ColumnMeta.INTEGER);
		helper.set("user002", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.DELETE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user001", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
		
		preparedStatement = provider.get(user, PreparedStatementType.DELETE);
		helper = new RecordstoreBindHelper(new SqliteDialect()).create(preparedStatement);
		helper.set("user002", ColumnMeta.STRING);
		ret = preparedStatement.executeUpdate();
		assertThat(ret, is(1));
	}
}