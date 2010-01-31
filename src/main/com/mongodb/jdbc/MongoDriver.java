// MongoDriver.java

package com.mongodb.jdbc;

import Zql.*;
import java.io.*;
import com.mongodb.*;

public class MongoDriver {

    public MongoDriver( DB db ){
        _db = db;
    }

    // ---- public methods -----

    DBCursor query( String sql )
        throws Exception {
        ZStatement st = parse( sql );
        if ( ! ( st instanceof ZQuery ) )
            throw new IllegalArgumentException( "not a query sql statement" );
        
        ZQuery q = (ZQuery)st;
        
        if ( q.getFrom().size() != 1 )
            throw new IllegalArgumentException( "can't handle joins" );
        DBCollection coll = _db.getCollection( q.getFrom().get(0).toString() );

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
        
        BasicDBObject query = new BasicDBObject();
        if ( q.getWhere() != null ){
            if ( ! ( q.getWhere() instanceof ZExpression ) )
                throw new RuntimeException( "don't know how to handle where except ZExpression" );
            
            ZExpression e = (ZExpression)q.getWhere();
            appendOperator( query , e );
            
        }
        
        System.out.println( "\t" + "fields: " + fields );
        System.out.println( "\t" + "query : " + query );

        DBCursor c = coll.find( query , fields );

        return c;
    }

    // ---- helpers -----

    Object toConstant( ZExp e ){
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

    void appendOperator( BasicDBObject query , ZExpression e ){
        if ( e.getOperator().equals( "=" ) )
            query.put( e.getOperand(0).toString() , toConstant( e.getOperand(1) ) );
        else if ( e.getOperator().equals( "AND" ) ){
            appendOperator( query , (ZExpression)e.getOperand(0) );
            appendOperator( query , (ZExpression)e.getOperand(1) );
        }
        else
            throw new RuntimeException( "can't handle operator [" + e.getOperator() + "]" );
    }

    ZStatement parse( String s )
        throws Exception {
        s = s.trim();
        if ( ! s.endsWith( ";" ) )
            s += ";";
        ZqlParser p = new ZqlParser( new ByteArrayInputStream( s.getBytes() ) );
        ZStatement st = p.readStatement();
        return st;
    }

    final DB _db;

}
