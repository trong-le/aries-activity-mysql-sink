#Aries Integration for PostgreSQL Databases

This is an integration to store data in [PostgreSQL Databases](https://www.postgresql.org/).


##Configuration

###Schema
The name of the schema to insert data.
```javascript
"schema": "test_schema",
```

###Table
The name of the table.
```javascript
"table": "test_table",
```

###Drop
Set to true to drop the existing table and refill it, or false to insert as new data.
```javascript
"drop": true,
```

###Connection
* Host: The url of the postgreSQL database.
* Port: The port of the database.
* User: The username, used for authentication.
* Password: The password associated with the user account.
* Database: The database where data should be stored.
```javascript
"connection" : {
    "host" : "postgresqlurl.com",
    "port" : 5432,
    "user" : "root",
    "password" : "veryinsecure",
    "database" : "test_database"
},
```

###Example Config
```javascript
{
    "schema" : "test_schema",
    "table" : "test_table",
    "drop" : true,
    "connection" : {
        "host" : "redshifturl.com",
        "port" : 5439,
        "user" : "root",
        "password" : "verysecure",
        "database" : "test_database"
    }
}
```