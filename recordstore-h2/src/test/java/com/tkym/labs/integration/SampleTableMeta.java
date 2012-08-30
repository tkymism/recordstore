package com.tkym.labs.integration;

import com.tkym.labs.record.Record;
import com.tkym.labs.record.RecordKey;
import com.tkym.labs.record.TableMeta;

class SampleTableMeta {
	/**
	 * Test Data: User
	 */
	static String PERSON_ID = "personId";
	static TableMeta PERSON = 
			TableMeta
				.table("person")
				.key(PERSON_ID).asLong()
				.column("firstName").asString()
				.column("familyName").asString()
				.column("birth").asInteger()
				.index("nameIdx").of("firstName","familyName")
				.index("birthIdx").of("birth")
				.meta();
	
	static Record user(long userId, String firstName, String familyName, int birth){
		Record record = new Record(new RecordKey(PERSON, userId));
		record.put("firstName", firstName);
		record.put("familuName", familyName);
		record.put("birth", birth);
		return record;
	}
	
	/**
	 * Test Data: Account
	 */
	static String EMAIL = "email";
	static TableMeta ACCOUNT = 
			TableMeta
				.table("account")
				.key(PERSON_ID).asLong()
				.key(EMAIL).asString()
				.column("profile1").asString(100)
				.column("profile2").asString(100)
				.column("profile3").asString(100)
				.meta();
	static Record account(long userId, String email, String profile1, String profile2, String profile3){
		Record record = new Record(new RecordKey(ACCOUNT, userId, email));
		record.put("profile1", profile1);
		record.put("profile2", profile2);
		record.put("profile3", profile3);
		return record;
	}
	
	/**
	 * Test Data: Account
	 */
	static String PAY_NO = "payNo";
	static TableMeta PAYMENT = 
			TableMeta
				.table("payment")
				.key(PERSON_ID).asLong()
				.key(EMAIL).asString()
				.key(PAY_NO).asShort()
				.column("amount").asDouble()
				.column("cash").asBoolean()
				.meta();
	static Record payment(long userId, String email, short payNo, double amount, boolean cash){
		Record record = new Record(new RecordKey(PAYMENT, userId, email, payNo));
		record.put("amount", amount);
		record.put("cash", cash);
		return record;
	}

	/**
	 * Test Data: BigData
	 */
	static String DATA_ID = "dataId";
	static TableMeta BIGDATA = 
			TableMeta
				.table("bigdata")
				.key(PERSON_ID).asLong()
				.key(DATA_ID  ).asLong()
				.column("data").asBytes()
				.meta();
	static Record bigdata(long userId, long dataNo, byte[] data){
		Record record = new Record(new RecordKey(BIGDATA, userId, dataNo));
		record.put("data", data);
		return record;
	}
}