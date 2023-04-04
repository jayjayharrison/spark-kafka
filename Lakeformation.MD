1. When you register a Data lake locations(S3 bucket), the bucket start to be managed by Lakeformation,  any delta table point to such location will return query error "NOT_SUPPORTED: Lake Formation tables are not supported for Delta Lake"
2. when create table on the Data lake locations, it will only work with the database that are point to the same location. But you can create table on the non Data lake locations bucket under any database
3. 


### Data lake locations: The S3 path which Lake formation will start to manage and apply its permission layer. If you don't specify the Data Lake Location, Lakeformation permission management will not do anything. Without this, you can create any table point to any location.

### Data Location Permission: Give specific role the permission to a Data Lake Location, this will allow role to Create Table using that location. 

### When creating a database, you can optionally specific the Data Location, this eliminate the need of assigning Data Location Permission to a role. The role can create table as long as the S3 location match the database s3 location.
