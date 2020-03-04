

|c1|
|---|
|"[{'cast_id': 14, 'character': 'Woody (voice)'},{'cast_id': 14, 'character': 'Woody (voice)'}]"|

1. trim '[' and ']' bracket 
2. replace , at each json object to ~ so that split can be performed correctlly
3. split on ~
4. replace '  to " , for hive to recognized it as json. if single quote, the get_json_object will return null

```
select get_json_object(exploded_tbl.json_column,"$.cast_id"),get_json_object(exploded_tbl.json_column,"$.character") from
(
select nm, regexp_replace(tbl.json1,"\'","\"") as json_column from 
(select split(arr,'~') as arr2, "jay" as nm from 
(select regexp_replace(regexp_replace(c1,'\\[|\\]',''),'\\},\\{','\\}\\~\\{') as arr from test2)t1
)t2
lateral view outer explode(arr2) tbl as json1
)exploded_tbl
;
```
