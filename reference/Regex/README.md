### 1) match comma that are not encloused by quote
,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)         => match , that are follow by abc"some string"

1,"wei,jay"  
