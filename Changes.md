# Changes

This document outlines the changes to the NHANES container as we transition to MariaDB 
from SQL Server.

## No Database Schemas

MariaDB does not support the concept of DB schemas as a namespace inside of a database.
The old NhanesLandingZone.Raw / Translated / Metadata will now each need to be their
own separate database.  We now have four separate databases:

- NhanesRaw
- NhanesTranslated
- NhanesMetadata
- NhanesOntology

In working through the initial refactoring of the download script, I found it easiest to
use VS Code to change all occurrences of `NhanesLandingZone`, delete `LandingZone` and the 
trailing `.`.  This takes, for example `NhanesLandingZone.Raw` to `NhanesRaw`

## New ODBC Drivers and R Database Package

Use `RMariaDB` to establish connections instead of `MsSqlTools` and `ODBC Driver 17 for SQL Server`.
For example:

```
RMariaDB::dbConnect(
    drv=RMariaDB::MariaDB(),
    username=sqlUserName,
    password=sqlPassword,
    host=sqlHost
)
```

While refactoring the download script, I found it easiest to use VS Code to change all occurrences of `MsSqlTools`
to 'DBI`, then change all `dbSendUpdate` to `dbExecute`.

## New DB Login Credentials

Username: admin
Password: C0lumnStore!

## Declaring Columnstore Tables in MariaDB
Instead of creating `columnstore index`es from `heap` tables in SQL Server, 
MariaDB uses an entirely separate engine to manage tables as coumnstores.
The table is a columnstore from it's inception.  This is done by specifying 
`ENGINE=ColumnStore` at the end of a `CREATE TABLE` statement.

See: https://mariadb.com/docs/server/sql/statements/schema/tables/enterprise-columnstore/create-table/

`INSERT` performance is particularly bad against these data strucutres, so it is 
critical to use one of the techniques described here to move large amounts of data:

https://mariadb.com/docs/server/data-operations/data-import/enterprise-columnstore/

In particular, data to be loaded from a text file on the filesystem of the database
server should be loaded via `LOAD DATA INFILE`, and moving data between
tables in the database instance should be accomplished with `INSERT .. SELECT`.

## Other Thoughts
- Instead of SQL Server's `DBCC SHRINKFILE`, issue `FLUSH BINARY LOG` to keep the 
transaction log tidy.

- There is no notion of a `CHECKPOINT` in MariaDB, so those statements can be removed.