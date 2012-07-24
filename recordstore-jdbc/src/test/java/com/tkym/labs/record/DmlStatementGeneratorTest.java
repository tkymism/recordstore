package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.tkym.labs.record.DMLStatementGenerator;
import com.tkym.labs.record.DefaultDmlStatementGenerator.SqlStatementGenerateHelper;
import com.tkym.labs.record.TableMeta.ColumnMeta;


public class DmlStatementGeneratorTest {
	
	/**
	 * @return
	 */
	static TableMeta asAccount(){
		return TableMeta
				.table("account")
				.key("personId").type(ColumnMeta.LONG)
				.key("email").type(ColumnMeta.STRING)
				.column("firstName").type(ColumnMeta.STRING)
				.column("familyName").type(ColumnMeta.STRING)
				.column("age").type(ColumnMeta.LONG)
				.meta();
	}
	
	@Test
	public void testSqlStatementHelperCase001(){
		SqlStatementGenerateHelper helper = new SqlStatementGenerateHelper();
		String result = helper.values(asAccount());
		assertThat(result, is("values (?, ?, ?, ?, ?)"));
	}
	
	@Test
	public void testSqlStatementHelperCaseInsert(){
		DMLStatementGenerator generator = new DefaultDmlStatementGenerator2();
		String result = generator.insert(asAccount());
		assertThat(result, is("insert into account (personId, email, firstName, familyName, age) values (?, ?, ?, ?, ?)"));
	}
	
	@Test
	public void testSqlStatementHelperCaseUpdate(){
		DMLStatementGenerator generator = new DefaultDmlStatementGenerator2();
		String result = generator.update(asAccount());
		assertThat(result, is("update account set firstName=?, familyName=?, age=? where personId=? and email=?"));
	}
	
	@Test
	public void testDMLStatementGeneratorCaseDelete(){
		DMLStatementGenerator generator = new DefaultDmlStatementGenerator2();
		String result = generator.delete(asAccount());
		assertThat(result, is("delete from account where personId=? and email=?"));
	}
}
