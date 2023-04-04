1. When you register a Data lake locations(S3 bucket), the bucket start to be managed by Lakeformation,  any delta table point to such location will return query error "NOT_SUPPORTED: Lake Formation tables are not supported for Delta Lake"
2. when create table on the Data lake locations, it will only work with the database that are point to the same location. But you can create table on the non Data lake locations bucket under any database
3. 
