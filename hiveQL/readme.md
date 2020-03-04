### dealing with nested json 
|c1|
|---|
|"[{'cast_id': 14, 'character': 'Woody (voice)'},{'cast_id': 14, 'character': 'Woody (voice)'}]"|

```
select nm, tbl.arr from 
(select split(arr,'~') as arr2, "jay" as nm from 
(select regexp_replace(regexp_replace(c1,'\\[|\\]',''),'\\},\\{','\\}\\~\\{') as arr from test2)t1
)t2
lateral view outer explode(arr2) tbl as arr --outer key word will keep row even if there is not array to explode
;
```
