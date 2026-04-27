@{concat('SELECT * FROM ', item().SchemaName, '.',item().TableName )}

As the ForEach loop iterates, it dynamically generates the SQL query to extract each table automatically.