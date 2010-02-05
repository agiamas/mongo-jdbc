// MongoDriver.java

package com.mongodb.jdbc;

import java.io.*;
import com.mongodb.*;

public class MongoDriver {

    public MongoDriver( DB db ){
        _db = db;
    }

    final DB _db;

}
