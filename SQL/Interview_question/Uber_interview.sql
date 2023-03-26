# Uber SQL Interview questions
create table binary_tree
(
    node int,
    parent int
);
select * from binary_tree;
insert into binary_tree values (5,8),(9,8),(4,5),(2,9),(1,5),(3,9),(8,null);
select node,
       CASE
            when node not in (select distinct parent from binary_tree where parent is not null) then 'LEAF'
            when parent is null then 'ROOT'
            else 'INNER'
       END as node_type
from binary_tree;
