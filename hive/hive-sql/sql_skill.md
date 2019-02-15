####拆分字段(行转列)

```
select bi_udf:bi_split_rows('cust', 'branch', 'ord1,ord2,ord3', ',') as (col1, col2, ord)
```

####多个维度聚合/卷积

```
select branch, fy, area, sum(val) as sum_val
from (
    select t1.branch, t2.fy, t3.area, t.val
    from (
        --构建实例数据
        select '深圳子公司' as branch,'FY19' as fy,'华南大区' as area,'1' as val from dual 
        union all 
        select '浙江子公司' as branch,'FY19' as fy,'华东大区' as area,'2' as val from dual 
        union all 
        select '天津子公司' as branch,'FY19' as fy,'华北大区' as area,'3' as val from dual 
        union all 
        select '浙江子公司' as branch,'FY18' as fy,'华东大区' as area,'4' as val from dual 
    ) t 
    lateral view explode(array('全部', t.branch)) t1 as branch
    lateral view explode(array('累计', t.fy)) t2 as fy
    lateral view explode(array('全国', t.area)) t3 as area
) t
group by branch, fy, area
```

####多层嵌套的json取值
**方法：使用get_json_object函数取出对象，使用 bi_udf:bi_json_array(array(*),  "key1",  " key2") as (key1, key2) 获取数组**
``` 
{
    "A":{
        "B":{
            "C":[
                {
                    "j1":"1",
                    "j2":"2"
                },
                {
                    "j1":"3",
                    "j2":"4"
                }
            ]
        }
    }
}

select get_json_object('{"A": {"B": {"C": [{"j1": "1111"},{"j2": "2222"}]}}}', '$.A.B.C[0].j1');  --取出C的第一个
select bi_udf:bi_json_array(get_json_object('{"A": {"B": {"C": [{"j1": "1", "j2": "2"},{"j1": "3", "j2": "4"}]}}}', '$.A.B.C[*]'), 'j1', 'j2') as (j1, j2); --取出数组
```

####取出正则表达式匹配的字符
方法：使用函数 regexp_extract(源字符串, 匹配表达式，返回第几个) ，正则表达式学习可以上度娘
``` 
select regexp_extract('http://www.alibaba.com?test1=123&id=111&ord=555', '&id=(\\d+)&', 1)  --输出：111
select regexp_extract('foothebar', 'foo(.*?)(bar)', 1) --输出：the
```

####正则匹配中文的问题
``` 
--hive匹配中文
select '我们来了' regexp "[\\x{4e00}-\\x{9fa5}]{3,}";
--替换中文为空
select regexp_replace('我们来了1234','[\\x{4e00}-\\x{9fa5}]','');
```

####多列转多行--【列转行】
方法：先将结果值拼接成字符串，而后使用trans_array(num, splitor, col1, col2, ..., coln )函数，将第num个之后的所有列（col1往后数）拆成多行，可配合bi_group_concat_order做拼接
``` 
select trans_array(0, ',', 列名, 值) as (列, 值)
from (
    select '收票未收齐,下户核查,禁止准入,审核不通过,未完成备案'                             as 列名
            ,concat(收票未收齐,',',下户核查,',',禁止准入,',',审核不通过,',',未完成备案)     as 值
    from (
        select 0.5      as  收票未收齐
                ,0.8    as  下户核查 
                ,0.35   as  禁止准入         
                ,0.2    as  审核不通过
                ,0.4    as  未完成备案
        from dual
    ) a
) t
```

####列转单行
``` 
select name,concat_ws(',',collect_set(concat(subject,'=',score))) from student group by name;
```

####一批数据随机抽3000条做样本
方法：随机抽样方法很多，比如取模等，这里使用rand()/rand(seed)生成随机数然后取前3000行，seed：可选参数，Bigint类型，随机数种子，决定随机数序列的起始值
``` 
select uid 
from (
    select uid
            ,row_number() over(order by rand(uid)) as n 
    from uid_table
) t 
where t.rn <= 3000
```

####遍历key不固定的json串，不能用常规的get_json_object
方法：使用bi_udf:bi_explode_key_value(col1, ..., coln, split1, split2) , 用split1对coln先进行切分，然后对切分后的结果用split2切分为多行key - value对，每行和coln前的列组合为多行
``` 
{"70167":6,"67490":10,"60890":45,"60691":9} //订单被哪些规则命中(规则ID：版本),先替换掉字符串{}和"

select bi_udf:bi_explode_key_value('test', '{"70167":6,"67490":10,"60890":45,"60691":9}'.replace('{','').replace('}', '').replace('"', ''), ',', ':') 
            as (col1, rule_uuid, revision)
```

####相同的子查询SQL写了很多遍，普遍的做法是创建中间表/临时表，这里使用CTE的方式无需建表，使代码更简洁
方法: 使用CTE语法 with cte_name AS (cte_query) [,cte_name2 AS (cte_query2), ……]
``` 
//使用CTE的语法, with 为前缀
with 
    a   as  (select distinct cust_id from x.x1 where ds=max_pt('x.x1') and qualification_status = 'DENY_AUDIT'), //复用结果
    b   as  (select a.cust_id,sum(clrc_amt) as gmv_amt,0 as prod_cnt  //获取GMV
             from a 
             join x.x2 b 
             on b.ds = max_pt('x.x2')
             and a.cust_id = b.cust_id
             group by a.cust_id
            ),
    c   as  (select a.cust_id,0.0 as gmv_amt,count(distinct prod_seq) as prod_cnt //获取产品数量
             from a 
             join x.x3 b 
             on b.ds = max_pt('x.x3')
             and a.cust_id = b.cust_id
             group by a.cust_id
            ),
    d   as  (select cust_id, max(gmv_amt) as gmv_amt, max(prod_cnt) as prod_cnt  //合并结果
             from (select cust_id, gmv_amt, prod_cnt from b union all select cust_id, gmv_amt, prod_cnt from c) t
             group by cust_id
    ) 
select cust_id, gmv_amt, prod_cnt from d; //输出
```

####使用参数化视图让SQL抽象成类似函数一样简洁【定制版，不展开，仅提供思路】
方法：语法create view if not exists pv1(@a table (k string,v bigint), @b string), 参数可以是任意表或者其他变量

####同一张表，多种聚合条件（类似group by 1,2,3 + group by 1,2 + group by 1,3），然后将结果合并在一起
方法：使用grouping sets实现多个聚合条件，SQL只跑一次，减少耗时
``` 
select stat_month                               //月份
        ,case when hscode_type is null and hscode is null   then '汇总'
            when hscode_type is not null                    then '行业'
            else '编码'
        end as flag                             //标识类型
        ,case when hscode_type is null and hscode is null   then '0'
            when hscode_type is not null                    then hscode_type
            else hscode
        end as hscode                           //行业、海关编码
        ,count(distinct ord_id) as ord_cnt      //订单量
from xx.xx1
where ds = '20190120' 
and stat_month = '2019-01'
group by stat_month, hscode_type, hscode        //要列出所有聚合的字段
grouping sets((stat_month), (stat_month,hscode_type), (stat_month,hscode))   //这句是重点，分3组
```