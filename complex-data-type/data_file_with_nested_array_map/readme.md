Reference: Hadoop The Definitive Guide - Chapter 12: Hive, Page No: 433, 434

#### hive has default field, collection, map delimiter. you can only define you own delimiter(up to 3 delimiters)

```
data.txt
id, name, dept_id,  phone
1,  jay,  58|14|21, mobile:6465254455|6468879898|work:7189895485|5729986565

// field separated by ,
// array in dept_id separated by |
// phone field should also by a array of map of {"mobile":"6465254455","6468879898") and {"work":"7189895485","5729986565"}
// this data.txt will not work because mobile map contain its nested array 
// so delimiter between 6468879898|work must NOT be '|', instead replace it with '\004' (hive will treat it as 4th delimiter)

data.txt
id, name, dept_id,  phone
1,  jay,  58|14|21, mobile:6465254455'\004'6468879898|work:7189895485'\004'5729986565

sql(s"""create external TABLE table600(ID bigint, NM string,depid ARRAY<String>,phone MAP<String, ARRAY<String>>) 
row format DELIMITED 
fields TERMINATED by ',' 
collection items terminated by '|' 
map keys terminated by ':' 
stored as textFILE location '/home/ec2-user/hive_external' """)


select phone[mobile] from table600;

```
```
Create table test_stg(employee_id INT, name STRING, abu ARRAY <String>, sabu MAP <String, ARRAY<INT>>)
row format delimited fields terminated by '|'                                                              
collection items terminated by '/'                                                                         
map keys terminated by ':'; 



Hive's default delimiters are:

Row Delimiter => Control-A ('\001')

Collection Item Delimiter => Control-B ('\002')

Map Key Delimiter => Control-C ('\003')

If you override these delimiters then overridden delimiters are used during parsing. The preceding description of delimiters is correct for the usual case of flat data structures, where the complex types only contain primitive types. For nested types the level of the nesting determines the delimiter.

For an array of arrays, for example, the delimiters for the outer array are Control-B ('\002') characters, as expected, but for the inner array they are Control-C ('\003') characters, the next delimiter in the list.

Hive actually supports eight levels of delimiters, corresponding to ASCII codes 1, 2, ... 8, but you can only override the first three.

For your case delimiter for items in nested Array of Map datatype field sabu will be '\004' as Map Key Delimiter is '\003' (Overridden as ':').

So you can write your input file as following format:

1|JOHN|abu1/abu2|key1:1'\004'2'\004'3/key2:6'\004'7'\004'8
Output of SELECT * FROM test_stg; will be:

1       JOHN     ["abu1","abu2"]     {"key1":[1,2,3],"key2":[6,7,8]}
Reference: Hadoop The Definitive Guide - Chapter 12: Hive, Page No: 433, 434
```
