// Executor.java

package com.mongodb.jdbc;

import Zql.*;
import java.io.*;
import java.util.*;
import com.mongodb.*;

public class Executor {

    static final boolean D = false;

    static DBCursor query( DB db , String sql )
        throws MongoSQLException {
        if ( D ) System.out.println( sql );
        
        ZStatement st = parse( sql );
        if ( ! ( st instanceof ZQuery ) )
            throw new IllegalArgumentException( "not a query sql statement" );
        
        ZQuery q = (ZQuery)st;
        
        if ( q.getFrom().size() != 1 )
            throw new IllegalArgumentException( "can't handle joins" );
        DBCollection coll = db.getCollection( q.getFrom().get(0).toString() );

        BasicDBObject fields = null;
        if ( q.getSelect().size() > 0 ){
            fields = new BasicDBObject();
            for ( int i=0; i<q.getSelect().size(); i++ ){
                ZSelectItem si = (ZSelectItem)q.getSelect().get(i);
                if ( si.isWildcard() )
                    break;
                fields.put( si.getColumn() , 1 );
            }
        }
        
        DBObject query = parseWhere( q.getWhere() );
        
        if ( D ) System.out.println( "\t" + "fields: " + fields );
        if ( D ) System.out.println( "\t" + "query : " + query );

        DBCursor c = coll.find( query , fields );

        return c;
    }

    static int writeop( DB db , String sql )
        throws MongoSQLException {
        
        if ( D ) System.out.println( sql );
        
        ZStatement st = parse( sql );
        if ( st instanceof ZInsert )
            return insert( db , (ZInsert)st );
        else if ( st instanceof ZUpdate )
            return update( db , (ZUpdate)st );

        throw new RuntimeException( "unknown write: " + st.getClass().toString() );
    }
    
    static int insert( DB db , ZInsert in )
        throws MongoSQLException {

        if ( in.getColumns() == null )
            throw new MongoSQLException.BadSQL( "have to give column names to insert" );
        
        if ( in.getColumns().size() != in.getValues().size() )
            throw new MongoSQLException.BadSQL( "number of values and columns have to match" );
        
        BasicDBObject o = new BasicDBObject();
        for ( int i=0; i<in.getColumns().size(); i++ ){
            Object c = in.getColumns().get(i);
            Object v = in.getValues().get(i);
            o.put( c.toString() , toConstant( (ZExp)v ) );
        }
        
        DBCollection coll = db.getCollection( in.getTable() );
        coll.insert( o );
        return 1; // TODO
    }

    static int update( DB db , ZUpdate up )
        throws MongoSQLException {
        
        DBObject query = parseWhere( up.getWhere() );
        
        BasicDBObject set = new BasicDBObject();
        Set<Map.Entry> changes = up.getSet().entrySet();
        for ( Map.Entry e : changes ){
            Object k = e.getKey();
            Object v = e.getValue();
            set.put( k.toString() , toConstant( (ZExp)v ) );
        }

        DBObject mod = new BasicDBObject( "$set" , set );

        DBCollection coll = db.getCollection( up.getTable() );
        coll.update( query , mod );
        return 1; // TODO
    }

    // ---- helpers -----

    static DBObject parseWhere( ZExp where ){
        BasicDBObject query = new BasicDBObject();
        if ( where == null )
            return query;
        
        if ( ! ( where instanceof ZExpression ) )
            throw new RuntimeException( "don't know how to handle where except ZExpression" );
        
        ZExpression e = (ZExpression)where;
        appendOperator( query , e );
        
        return query;
    }

    static Object toConstant( ZExp e ){
        if ( ! ( e instanceof ZConstant ) )
            throw new IllegalArgumentException( "toConstant needs a ZConstant" );
        ZConstant c = (ZConstant)e;
        switch ( c.getType() ){
        case ZConstant.COLUMNNAME:
        case ZConstant.STRING:
        case ZConstant.UNKNOWN:
            return c.getValue();
        case ZConstant.NUMBER:
            return Integer.parseInt( c.getValue() );
        case ZConstant.NULL:
            return null;
        }
        throw new IllegalArgumentException( "what [" + e + "]" );
    }

    static void appendOperator( BasicDBObject query , ZExpression e ){
        if ( e.getOperator().equals( "=" ) )
            query.put( e.getOperand(0).toString() , toConstant( e.getOperand(1) ) );
        else if ( e.getOperator().equals( "AND" ) ){
            appendOperator( query , (ZExpression)e.getOperand(0) );
            appendOperator( query , (ZExpression)e.getOperand(1) );
        }
        else
            throw new RuntimeException( "can't handle operator [" + e.getOperator() + "]" );
    }
    
    static ZStatement parse( String s )
        throws MongoSQLException {
        s = s.trim();
        if ( ! s.endsWith( ";" ) )
            s += ";";
        try {
            ZqlParser p = new ZqlParser( new ByteArrayInputStream( s.getBytes() ) );
            return p.readStatement();
        }
        catch ( Exception e ){
            throw new MongoSQLException.BadSQL( s );
        }
    }

}
