
Mongo JDBC Driver
===========

__EXPERIMENTAL__

This is an experimant JDBC driver for MongoDB.  It attempts to map some basic SQL to MongoDB syntax.  
One of the interesting things is that if you use prepared statements, you can actually use embedded objects, etc... quite nicely. 
See examples/ for more info, ideas.

Home: http://github.com/erh/mongo-jdbc/


### Supported
 - SELECT
   - field selector
   - order by
 - INSERT
 - UPDATE
   - basics
 - DROP

### TODO
 - insert & getLastError
 - embedded objects
 - prepared statements
