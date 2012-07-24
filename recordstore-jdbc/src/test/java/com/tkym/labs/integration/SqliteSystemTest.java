package com.tkym.labs.integration;

import static com.tkym.labs.integration.SampleTableMeta.ACCOUNT;
import static com.tkym.labs.integration.SampleTableMeta.PAYMENT;
import static com.tkym.labs.integration.SampleTableMeta.PERSON;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import com.tkym.labs.record.Record;
import com.tkym.labs.record.RecordstoreException;
import com.tkym.labs.record.RecordstoreService;
import com.tkym.labs.record.RecordstoreServiceFactory;
import com.tkym.labs.record.TableMeta;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.SqliteRecordstoreServiceFactory;


public class SqliteSystemTest {
	private static RecordstoreService service;
	@BeforeClass
	public static void setupClass() throws RecordstoreException{
		RecordstoreServiceFactory factory = new SqliteRecordstoreServiceFactory();
		service = factory.create();
	}
	@AfterClass
	public static void teardownClass() throws RecordstoreException{
		service.getTransaction().close();
	}
	
	TableMeta MASTER = 
			TableMeta.table("sqlite_master").
			column("name").asString().
			column("tbl_name").asString().
			column("type").asString().
			column("rootpage").asLong().
			column("sql").asString().
			meta();
	
	@Test
	public void testSystemDataQueryCase001() throws RecordstoreException{
		service.create(PERSON, true);
		service.create(ACCOUNT, true);
		service.create(PAYMENT, true);
		List<Record> list =  service.query(MASTER).record().asList();
		for(Record record : list)
			System.out.println(
					record.get("name")+":"+
					record.get("tbl_name")+":"+
					record.get("type")+":"+
					record.get("rootpage")+":"+
					record.get("sql")
					);
		assertThat(list.size(), is(3));
		Assert.assertThat(list.get(0).get("tbl_name",String.class), is(PERSON.tableName())); 
		Assert.assertThat(list.get(1).get("tbl_name",String.class), is(ACCOUNT.tableName())); 
		Assert.assertThat(list.get(2).get("tbl_name",String.class), is(PAYMENT.tableName())); 
		service.drop(PERSON, true);
		service.drop(ACCOUNT, true);
		service.drop(PAYMENT, true);
	}
}