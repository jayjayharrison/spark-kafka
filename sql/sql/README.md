
# postgres
# https://www.lakshminp.com/building-nested-hierarchy-json-relational-db


```
CREATE TABLE tags (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    parent_id integer
);


INSERT INTO tags VALUES (7, 'Ciencia', NULL);
INSERT INTO tags VALUES (8, 'Biología', 7);
INSERT INTO tags VALUES (9, 'Química', 7);
INSERT INTO tags VALUES (10, 'Física', 7);
INSERT INTO tags VALUES (11, 'Geología', 7);
INSERT INTO tags VALUES (12, 'Psicología', 7);
INSERT INTO tags VALUES (18, 'Etología', 8);
INSERT INTO tags VALUES (19, 'Botánica', 8);
INSERT INTO tags VALUES (20, 'Animalitos', 18);



WITH RECURSIVE tagtree AS (
  SELECT tags.name, tags.parent_id, tags.id, json '[]' children
  FROM tags
  WHERE NOT EXISTS (SELECT 1 FROM tags tt WHERE tt.parent_id = tags.id)

  UNION ALL

  SELECT (tags).name, (tags).parent_id, (tags).id, array_to_json(array_agg(tagtree)) children FROM (
    SELECT tags, tagtree
    FROM tagtree
    JOIN tags ON tagtree.parent_id = tags.id
  ) v
  GROUP BY v.tags
)

SELECT array_to_json(array_agg(tagtree)) json
FROM tagtree
WHERE parent_id IS NULL;


```

```
CREATE OR REPLACE FUNCTION get_children(tag_id integer)
RETURNS json AS $$
DECLARE
result json;
BEGIN
SELECT array_to_json(array_agg(row_to_json(t))) INTO result
    FROM (
WITH RECURSIVE tree AS (
  SELECT id, name, ARRAY[]::INTEGER[] AS ancestors
  FROM tags WHERE parent_id IS NULL

  UNION ALL

  SELECT tags.id, tags.name, tree.ancestors || tags.parent_id
  FROM tags, tree
  WHERE tags.parent_id = tree.id
) SELECT id, name, ARRAY[]::INTEGER[] AS children FROM tree WHERE $1 = tree.ancestors[array_upper(tree.ancestors,1)]
) t;
RETURN result;
END;
$$ LANGUAGE plpgsql;


```
```
CREATE OR REPLACE FUNCTION get_tree(data json) RETURNS json AS $$

var root = [];

for(var i in data) {
  build_tree(data[i]['id'], data[i]['name'], data[i]['children']);
}

function build_tree(id, name, children) {
  var exists = getObject(root, id);
  if(exists) {
       exists['children'] = children;
  }
  else {
    root.push({'id': id, 'name': name, 'children': children});
  }
}


function getObject(theObject, id) {
    var result = null;
    if(theObject instanceof Array) {
        for(var i = 0; i < theObject.length; i++) {
            result = getObject(theObject[i], id);
            if (result) {
                break;
            }   
        }
    }
    else
    {
        for(var prop in theObject) {
            if(prop == 'id') {
                if(theObject[prop] === id) {
                    return theObject;
                }
            }
            if(theObject[prop] instanceof Object || theObject[prop] instanceof Array) {
                result = getObject(theObject[prop], id);
                if (result) {
                    break;
                }
            } 
        }
    }
    return result;
}


    return JSON.stringify(root);
$$ LANGUAGE plv8 IMMUTABLE STRICT;

```


## Player with Longest Streak
```
select * , (rn1 - rn2) as streak_id
from
(
select *, 
row_number() over(partition by player_id order by match_date) as rn1,
row_number() over(partition by player_id,match_result order by match_date) as rn2
from players_results
)t0
order by player_id, match_date;

-- -------------------

or populate result change indicator, using result != lag(result) as streak_change,
and sum the streak_change over player id partition order by date asc
the sum will have window range of top row to current row, so when there is a streak change, sum is added up by one. when no change, it mean the result is the same as previous result, so sum stay the same. 
this sum can serve as a streak id


```
