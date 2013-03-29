recordstore
===========

recordstore is database framework libraly for Java framework.

## Usage

### Define TableMeta

```java
static TableMeta PERSON = 
		TableMeta
			.table("person")                // define a table name.
			.key(PERSON_ID).asLong()        // define primary keys.
			.column("firstName").asString() // define columns
			.column("familyName").asString()
			.column("birth").asInteger()
			.index("nameIdx").of("firstName","familyName")  // define index
			.index("birthIdx").of("birth")
			.meta();                        // build.
```
### Create Instance
```java
  Record createInstance(long userId, String firstName, String familyName){
    return new Record(new RecordKey(PERSON, 1L));
  }
```

### Operate Record
```java
  private RecordstoreService service;
  
  void testRecordOperation(){
    Record rec = createRecord(1001L, "foo", "bar");
    
    service.put(rec);                       // insert record.
    Record rec2 = service.get(rec.key());   // get record from Database.
    rec2.put("firstName", "foo2");
    service.put(rec2)                       // update record.
    service.deleteIfExists(rec2.key());     // delete record.
  }
```

### Query Record
```java
  void testQuery(){
    Iterator<Record> result = 
      service.query(PERSON).
        filter("firstName").startWith("f"). // define filter property.
        filter("birth").greaterThan(20).    
        asc("firstName").asc("familyName"). // define sort property.
        record().asIterator();              // getting results as Iterator.
  }
```

