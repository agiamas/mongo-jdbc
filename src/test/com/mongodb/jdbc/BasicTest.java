// BasicTest.java

package com.mongodb.jdbc;

import com.mongodb.*;
import org.testng.annotations.Test;

public class BasicTest {
    
    final Mongo _mongo;
    final DB _db;
    final MongoDriver _driver;
    
    public BasicTest()
        throws java.net.UnknownHostException {
        _mongo = new Mongo();
        _db = _mongo.getDB( "jdbctest" );
        _driver = new MongoDriver( _db );
    }
    
    @Test
    public void test1()
        throws Exception {
        System.out.println( _driver.query( "select * from foo" ).toArray() );
        System.out.println( _driver.query( "select a,b from foo" ).toArray() );
        System.out.println( _driver.query( "select a,b from foo where x=3" ).toArray() );

    }
}
