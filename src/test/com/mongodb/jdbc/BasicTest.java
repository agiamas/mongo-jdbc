// BasicTest.java

package com.mongodb.jdbc;

import com.mongodb.*;
import org.testng.annotations.Test;

public class BasicTest extends Base {
    
    final MongoDriver _driver;
    
    public BasicTest(){
        _driver = new MongoDriver( _db );
    }
    
    @Test
    public void test1()
        throws Exception {
        String name = "simple.test1";
        DBCollection c = _db.getCollection( name );
        c.drop();
        
        for ( int i=1; i<=3; i++ ){
            c.insert( BasicDBObjectBuilder.start( "a" , i ).add( "b" , i ).add( "x" , i ).get() );
        }
        
        DBObject empty = new BasicDBObject();
        DBObject ab = BasicDBObjectBuilder.start( "a" , 1 ).add( "b" , 1 ).get();
        
        assertEquals( c.find().toArray() , _driver.query( "select * from " + name ).toArray() );
        assertEquals( c.find( empty , ab ).toArray(), _driver.query( "select a,b from " + name ).toArray() );
        assertEquals( c.find( new BasicDBObject( "x" , 3 ) , ab ).toArray() , _driver.query( "select a,b from " + name + " where x=3" ).toArray() );

    }
}
