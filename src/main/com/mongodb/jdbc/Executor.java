// Executor.java

package com.mongodb.jdbc;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.update.*;

import com.mongodb.*;

public class Executor {

    static final boolean D = false;

    static DBCursor query( DB db , String sql )
        throws MongoSQLException {
        if ( D ) System.out.println( sql );
        
        Statement st = parse( sql );
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
        
        Statement st = parse( sql );
        if ( st instanceof Insert )
            return insert( db , (Insert)st );
        else if ( st instanceof Update )
            return update( db , (Update)st );

        throw new RuntimeException( "unknown write: " + st.getClass().toString() );
    }
    
    static int insert( DB db , Insert in )
        throws MongoSQLException {

        if ( in.getColumns() == null )
            throw new MongoSQLException.BadSQL( "have to give column names to insert" );
        
        DBCollection coll = db.getCollection( in.getTable().toString() );        
        if ( D ) System.out.println( "\t" + "table: " + coll );

        
        if ( ! ( in.getItemsList() instanceof ExpressionList ) )
            throw new UnsupportedOperationException( "need ExpressionList" );
        
        BasicDBObject o = new BasicDBObject();

        List valueList = ((ExpressionList)in.getItemsList()).getExpressions();
        if ( in.getColumns().size() != valueList.size() )
            throw new MongoSQLException.BadSQL( "number of values and columns have to match" );

        for ( int i=0; i<valueList.size(); i++ ){
            o.put( in.getColumns().get(i).toString() , toConstant( (Expression)valueList.get(i) ) );

        }

        coll.insert( o );        
        return 1; // TODO - this is wrong
    }

    static int update( DB db , Update up )
        throws MongoSQLException {
        
        DBObject query = parseWhere( up.getWhere() );
        
        BasicDBObject set = new BasicDBObject();
        
        for ( int i=0; i<up.getColumns().size(); i++ ){
            String k = up.getColumns().get(i).toString();
            Expression v = (Expression)(up.getExpressions().get(i));
            set.put( k.toString() , toConstant( v ) );
        }

        DBObject mod = new BasicDBObject( "$set" , set );

        DBCollection coll = db.getCollection( up.getTable().toString() );
        coll.update( query , mod );
        return 1; // TODO
    }

    // ---- helpers -----

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

    static Statement parse( String s )
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
