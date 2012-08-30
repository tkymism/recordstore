package com.tkym.labs.integration;

import static com.tkym.labs.integration.SampleTableMeta.ACCOUNT;
import static com.tkym.labs.integration.SampleTableMeta.PAYMENT;
import static com.tkym.labs.integration.SampleTableMeta.PERSON;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.H2RecordstoreServiceFactory;
import com.tkym.labs.record.RecordstoreException;
import com.tkym.labs.record.RecordstoreService;

public class H2RecordstoreFactoryTest {
	static final String JDBC_CLASS = "org.h2.Driver"; 

	@BeforeClass
	public static void setupClass() throws ClassNotFoundException{ Class.forName(JDBC_CLASS); }
	
//	@Test
	public void testH2() throws SQLException{
		Properties props = new Properties();
        props.put("user", "hoge");
        props.put("password", "hogehoge");
        String url = "jdbc:h2:db";
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(false);
        conn.prepareStatement("select * from person").executeQuery();
	}
	
	@Test
	public void testH2RecordstoreServiceFactory() throws RecordstoreException{
		H2RecordstoreServiceFactory factory = 
				new H2RecordstoreServiceFactory(
						H2RecordstoreFactoryTest.class.getResource("db"));
		RecordstoreService service = factory.create();
		service.create(PERSON, true);
		service.create(ACCOUNT, true);
		service.create(PAYMENT, true);
		service.drop(PERSON, false);
		service.drop(ACCOUNT, false);
		service.drop(PAYMENT, false);
	}
}