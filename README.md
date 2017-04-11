# ExampleApacheIgnite.NET-LoadCacheFromOracleDB-SQLQueryCompositePrimaryKey
Example of Apache Ignite where I want to load a cache from an Oracle DB table, overriden loadCache method. Then I want to make a sql query to the cache.

(Remember to change the variable  info in TestObjectCacheStore.cs, with your DB conecction parameters)

Ignite 1.9

Oracle 12c

.Net Framework 4.5

Example Table:

TESTOBJECT
PK: ID,NAME

| ID | NAME  | VALUE  |
|----|-------|--------
| 1  | h1    | val1   |
| 2  | h2    | val2   |
