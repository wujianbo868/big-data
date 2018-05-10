create table dm_crm.test_map_array(  
order_id string,  
movie array<map<string,string>> )
STORED AS ORC;

insert overwrite table dm_crm.test_map_array 
select name,collect_set(movie)
from dm_crm.test_map
group by name;
