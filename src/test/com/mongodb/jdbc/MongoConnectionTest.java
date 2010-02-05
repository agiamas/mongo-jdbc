// MongoConnectionTest.java

package com.mongodb.jdbc;

import java.sql.*;
import com.mongodb.*;
import org.testng.annotations.Test;

public class MongoConnectionTest extends Base {

    MongoConnection _conn;
    
    public MongoConnectionTest(){
        super();
        _conn = new MongoConnection( _db );
    }

    @Test
    public void testBasics()
        throws SQLException {
        
        String name = "conn.basic1";
        DBCollection coll = _db.getCollection( name );
        coll.drop();
        
        coll.insert( BasicDBObjectBuilder.start( "x" , 1 ).add( "y" , "foo" ).get() );
        coll.insert( BasicDBObjectBuilder.start( "x" , 2 ).add( "y" , "bar" ).get() );
        
        Statement stmt = _conn.createStatement();
        ResultSet res = stmt.executeQuery( "select * from " + name + " order by x" );
        assertTrue( res.next() );
        assertEquals( 1 , res.getInt("x" ) );
        assertEquals( "foo" , res.getString("y" ) );
        assertTrue( res.next() );
        assertEquals( 2 , res.getInt("x" ) );
        assertEquals( "bar" , res.getString("y" ) );
        assertFalse( res.next() );

        res.close();
        stmt.close();

    }
    
}
