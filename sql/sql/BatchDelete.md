## bulk delete run for a long time?
#### remove all foreign key relationship, remove all trigger , remove all on delete cascade

```

ALTER TABLE table_name DISABLE TRIGGER trigger_name | ALL
DELETE from ....
ALTER TABLE table_name ENABLE TRIGGER trigger_name | ALL

```

### do batch delete instead
##### run until not row are return by the cte
```
WITH delete_scope AS (
   SELECT id                 -- your PK
   FROM   tbl
   WHERE  date < $something  -- your condition
   -- ORDER BY ???           -- optional, see below
   LIMIT  5000               -- size of batch
   FOR    UPDATE             -- SKIP LOCKED ? If FOR UPDATE or FOR SHARE is specified, the SELECT statement locks the selected rows against concurrent updates.
   )
   
DELETE FROM tbl
USING  delete_scope
WHERE  tbl.id = delete_scope.id;


```

### using bash script,  implement a while loop https://lookonmyworks.co.uk/2020/10/08/deleting-data-in-batches/
```
WITH deleted AS (
    DELETE FROM foo
    WHERE id = any(array(SELECT id FROM foo WHERE <condition> limit 100)) 
    RETURNING id
)
SELECT count(*) FROM deleted;




#!/bin/bash
while :
do
        VALUE=$(psql -qtA -d $DBNAME -c "WITH deleted AS (DELETE FROM foo WHERE id = any(array(SELECT id FROM foo WHERE <condition> limit 100))  RETURNING id)")
        echo "deleted $VALUE"
        if [ $VALUE -eq 0 ]
        then
                break
        fi
        sleep 1
done


```
