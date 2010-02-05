// Executor.java

package com.mongodb.jdbc;

import java.io.*;
import java.util.*;

import Zql.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;

import com.mongodb.*;

public class Executor {

    static DBCursor query( DB db , String sql )
        throws MongoSQLException {
        if ( D ) System.out.println( sql );
        
        Statement st = parse2( sql );
        if ( ! ( st instanceof Select ) )
            throw new IllegalArgumentException( "not a query sql statement" );
        
        Select select = (Select)st;
        if ( ! ( select.getSelectBody() instanceof PlainSelect ) )
            throw new UnsupportedOperationException( "can only handle PlainSelect so far" );
        
        PlainSelect ps = (PlainSelect)select.getSelectBody();
        if ( ! ( ps.getFromItem() instanceof Table ) )
            throw new UnsupportedOperationException( "can only handle regular tables" );
        
        DBCollection coll = db.getCollection( ((Table)ps.getFromItem()).toString() );

        BasicDBObject fields = new BasicDBObject();
        for ( Object o : ps.getSelectItems() ){
            SelectItem si = (SelectItem)o;
            if ( si instanceof AllColumns ){
                if ( fields.size() > 0 )
                    throw new UnsupportedOperationException( "can't have * and fields" );
                break;
            }
            else if ( si instanceof SelectExpressionItem ){
                SelectExpressionItem sei = (SelectExpressionItem)si;
                fields.put( toFieldName( sei.getExpression() ) , 1 );
            }
            else {
                throw new UnsupportedOperationException( "unknown select item: " + si.getClass() );
            }
        }
        
        // where
        DBObject query = parseWhere( ps.getWhere() );
        
        // done with basics, build DBCursor
        if ( D ) System.out.println( "\t" + "table: " + coll );
        if ( D ) System.out.println( "\t" + "fields: " + fields );
        if ( D ) System.out.println( "\t" + "query : " + query );
        DBCursor c = coll.find( query , fields );
        
        { // order by
            List orderBylist = ps.getOrderByElements();
            if ( orderBylist != null && orderBylist.size() > 0 ){
                BasicDBObject order = new BasicDBObject();
                for ( int i=0; i<orderBylist.size(); i++ ){
                    OrderByElement o = (OrderByElement)orderBylist.get(i);
                    order.put( o.getColumnReference().toString() , o.isAsc() ? 1 : -1 );
                }
                c.sort( order );
            }
        }

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

    static String toFieldName( Expression e ){
        if ( e instanceof StringValue )
            return e.toString();
        if ( e instanceof Column )
            return e.toString();
        throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass() + " into field name" );
    }

    static Object toConstant( Expression e ){
        if ( e instanceof StringValue )
            return ((StringValue)e).getValue();
        else if ( e instanceof DoubleValue )
            return ((DoubleValue)e).getValue();
        else if ( e instanceof LongValue )
            return ((LongValue)e).getValue();
        else if ( e instanceof NullValue )
            return null;
        throw new UnsupportedOperationException( "can't turn [" + e + "] into constant " );
    }


    static DBObject parseWhere( Expression e ){
        BasicDBObject o = new BasicDBObject();
        if ( e == null )
            return o;
        
        if ( e instanceof EqualsTo ){
            EqualsTo eq = (EqualsTo)e;
            o.put( toFieldName( eq.getLeftExpression() ) , toConstant( eq.getRightExpression() ) );
        }
        else {
            throw new UnsupportedOperationException( "can't handle: " + e.getClass() + " yet" );
        }

        return o;
    }

    static Statement parse2( String s )
        throws MongoSQLException {
        s = s.trim();
        
        try {
            return (new CCJSqlParserManager()).parse( new StringReader( s ) );
        }
        catch ( Exception e ){
            throw new MongoSQLException.BadSQL( s );
        }
        
    }
    
}
