// MongoPreparedStatement.java

package com.mongodb.jdbc;

import java.math.*;
import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.net.*;

import com.mongodb.*;

public class MongoPreparedStatement extends MongoStatement implements PreparedStatement {

    MongoPreparedStatement( MongoConnection conn , int type, int concurrency, int holdability , String sql ){
        super( conn , type , concurrency , holdability );
        _sql = sql;
    }

    public void addBatch(){
        throw new UnsupportedOperationException( "batch stuff not supported" );
    }
    
    // --- metadata ---

    public ResultSetMetaData getMetaData(){
        throw new UnsupportedOperationException();
    }
    public ParameterMetaData getParameterMetaData(){
        throw new UnsupportedOperationException();
    }

    public void clearParameters(){
        throw new UnsupportedOperationException();
    }
    
    // ----- actually do
    
    public boolean execute(){
        throw new RuntimeException( "execute not done" );
    }
    
    public ResultSet executeQuery(){
        throw new RuntimeException( "executeQuery not done" );
    }
    
    public int executeUpdate(){
        throw new RuntimeException( "executeUpdate not done" );
    }

    // ---- setters -----

    public void setArray(int parameterIndex, Array x){ _setnotdone(); }
    public void setAsciiStream(int parameterIndex, InputStream x){ _setnotdone(); } 
    public void setAsciiStream(int parameterIndex, InputStream x, int length){ _setnotdone(); } 
    public void setAsciiStream(int parameterIndex, InputStream x, long length){ _setnotdone(); } 
    public void setBigDecimal(int parameterIndex, BigDecimal x){ _setnotdone(); } 
    public void setBinaryStream(int parameterIndex, InputStream x){ _setnotdone(); } 
    public void setBinaryStream(int parameterIndex, InputStream x, int length){ _setnotdone(); } 
    public void setBinaryStream(int parameterIndex, InputStream x, long length){ _setnotdone(); } 
    public void setBlob(int parameterIndex, Blob x){ _setnotdone(); } 
    public void setBlob(int parameterIndex, InputStream inputStream){ _setnotdone(); } 
    public void setBlob(int parameterIndex, InputStream inputStream, long length){ _setnotdone(); } 
    public void setBoolean(int parameterIndex, boolean x){ _setnotdone(); } 
    public void setByte(int parameterIndex, byte x){ _setnotdone(); } 
    public void setBytes(int parameterIndex, byte[] x){ _setnotdone(); } 
    public void setCharacterStream(int parameterIndex, Reader reader){ _setnotdone(); } 
    public void setCharacterStream(int parameterIndex, Reader reader, int length){ _setnotdone(); } 
    public void setCharacterStream(int parameterIndex, Reader reader, long length){ _setnotdone(); } 
    public void setClob(int parameterIndex, Clob x){ _setnotdone(); } 
    public void setClob(int parameterIndex, Reader reader){ _setnotdone(); } 
    public void setClob(int parameterIndex, Reader reader, long length){ _setnotdone(); } 
    public void setDate(int parameterIndex, Date x){ _setnotdone(); } 
    public void setDate(int parameterIndex, Date x, Calendar cal){ _setnotdone(); } 
    public void setDouble(int parameterIndex, double x){ _setnotdone(); } 
    public void setFloat(int parameterIndex, float x){ _setnotdone(); } 
    public void setInt(int parameterIndex, int x){ _setnotdone(); } 
    public void setLong(int parameterIndex, long x){ _setnotdone(); } 
    public void setNCharacterStream(int parameterIndex, Reader value){ _setnotdone(); } 
    public void setNCharacterStream(int parameterIndex, Reader value, long length){ _setnotdone(); } 
    public void setNClob(int parameterIndex, NClob value){ _setnotdone(); } 
    public void setNClob(int parameterIndex, Reader reader){ _setnotdone(); } 
    public void setNClob(int parameterIndex, Reader reader, long length){ _setnotdone(); } 
    public void setNString(int parameterIndex, String value){ _setnotdone(); } 
    public void setNull(int parameterIndex, int sqlType){ _setnotdone(); } 
    public void setNull(int parameterIndex, int sqlType, String typeName){ _setnotdone(); } 
    public void setObject(int parameterIndex, Object x){ _setnotdone(); } 
    public void setObject(int parameterIndex, Object x, int targetSqlType){ _setnotdone(); } 
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength){ _setnotdone(); } 
    public void setRef(int parameterIndex, Ref x){ _setnotdone(); } 
    public void setRowId(int parameterIndex, RowId x){ _setnotdone(); } 
    public void setShort(int parameterIndex, short x){ _setnotdone(); } 
    public void setSQLXML(int parameterIndex, SQLXML xmlObject){ _setnotdone(); } 
    public void setString(int parameterIndex, String x){ _setnotdone(); } 
    public void setTime(int parameterIndex, Time x){ _setnotdone(); } 
    public void setTime(int parameterIndex, Time x, Calendar cal){ _setnotdone(); } 
    public void setTimestamp(int parameterIndex, Timestamp x){ _setnotdone(); } 
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal){ _setnotdone(); } 
    public void setUnicodeStream(int parameterIndex, InputStream x, int length){ _setnotdone(); } 
    public void setURL(int parameterIndex, URL x){ _setnotdone(); } 

    void _setnotdone(){
        throw new UnsupportedOperationException( "setter not done" );
    }

    final String _sql;
}
