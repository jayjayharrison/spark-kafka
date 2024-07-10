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

### window function, add session id for every 30 min of inactivity

```
user,tmst
jay,2014-01-04 13:43:14.653
jay,2014-01-04 13:45:14.653
jay,2014-01-04 14:43:14.653
jay,2014-01-04 14:45:14.653
jay,2014-01-04 18:43:14.653
jay,2014-01-04 19:43:14.653

sql("create external table clickstream(user string,tmst timestamp) row format delimited fields terminated by ',' lines terminated by '\\n' location '/home/ec2-user/hive_external' ")

sql("""
select user,ts,last_ts,new_session new_session_indicator,
sum(if(new_session,1,0)) over (PARTITION BY user ORDER BY ts) as new_session_0,
sum(if(new_session,1,0)) over (PARTITION BY user ORDER BY ts) + 1 as session_id
from
(SELECT
    user, ts, LAG(ts) OVER w as last_ts
    ,ts - LAG(ts) OVER w > 1800 AS new_session ,ts2
  FROM (
    SELECT *, UNIX_TIMESTAMP(tmst) AS ts, tmst as ts2
    FROM clickstream
  ) a
  WINDOW w AS (PARTITION BY user ORDER BY tmst)
)b
""").show(false)

+----+----------+----------+---------------------+-------------+----------+
|user|ts        |last_ts   |new_session_indicator|new_session_0|session_id|
+----+----------+----------+---------------------+-------------+----------+
|jay |1388842994|null      |null                 |0            |1         |
|jay |1388843114|1388842994|false                |0            |1         |
|jay |1388846594|1388843114|true                 |1            |2         |
|jay |1388846714|1388846594|false                |1            |2         |
|jay |1388860994|1388846714|true                 |2            |3         |
|jay |1388864594|1388860994|true                 |3            |4         |
+----+----------+----------+---------------------+-------------+----------+


```
