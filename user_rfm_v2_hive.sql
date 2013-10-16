use yingzi;
show tables;

--yingzi.seg_raw
--yingzi.seg_rfm_info
--yingzi.user_application 
--yingzi.app_df
--yingzi.seg_rfm_tfidf_app
--yingzi.user_feature


-------------------------------------------------------------------------------------
select * from yingzi.seg_raw where period = '201301_201302_201303' limit 100;
--user_id	play_days	last_play	spend_days	spend_coin	last_spend	period
--1	4	20130327	1	47360	2013-03-26	201301_201302_201303
--7	1	20130328	0	0	<null>	201301_201302_201303
-------------------------------------------------------------------------------------

create table yingzi.seg_raw(
	user_id bigint,
	play_days int,
	last_play string,
	spend_days int,
	spend_coin int,
	last_spend string
) partitioned by (period string);
 
 
--insert into table sandbox.yingzi_seg_raw partition(period='201301_201302_201303')
--insert into table yingzi_seg_raw partition(period='201304_201305_201306')
insert into table yingzi.seg_raw partition(period='201307_201308')
select play.user_id
	,play.play_days
	,play.last_play
	,COALESCE(spend.spend_days,0) as spend_days
	,COALESCE(spend.spend_coin,0) as spend_coin
	,spend.last_spend
from 
(
	select user_id, count(distinct dt) as play_days, max(dt) as last_play
	from default.log_platform_ggp
	--where dt between '20130101' and '20130331' and user_info.region = 'JP' and device_id=2
	--where dt between '20130401' and '20130630' and user_info.region = 'JP' and device_id=2
	where dt between '20130701' and '20130831' and user_info.region = 'JP' and device_id=2
	group by user_id
) play
left outer join
(
	select user_id, count(distinct dt) as spend_days, sum(-nondeveloper_coin) as spend_coin, max(dt) as last_spend
	from default.gree_coin
	--where dt between '2013-01-01' and '2013-03-31' and type=-2010 and nondeveloper_coin<0
	--where dt between '2013-04-01' and '2013-06-30' and type=-2010 and nondeveloper_coin<0
	where dt between '2013-07-01' and '2013-08-31' and type=-2010 and nondeveloper_coin<0
	group by user_id
) spend
on (play.user_id=spend.user_id and play.user_id !=0)
 
 
show partitions yingzi.seg_raw;
 
select * from yingzi.seg_raw where period in ('201301_201302_201303','201304_201305_201306','201307_2013_08') and  play_days=0 limit 100
 
select count(distinct user_id) from yingzi.seg_raw where partitionperiod='201301_201302_201303'
 
---
create table tmp_jin3 as
select  user_id, collect_map(period, named_struct('play', play_days, 'coin', spend_coin)) as info
from yingzi_seg_raw where period is not null
group by user_id
limit 100;
 
/*
user_id	info
1	{"201307_201308":{"play":3,"coin":10},"201301_201302_201303":{"play":4,"coin":47360},"201304_201305_201306":{"play":8,"coin":10}}
5	{"201301_201302_201303":{"play":4,"coin":117000}}
7	{"201301_201302_201303":{"play":1,"coin":0},"201307_201308":{"play":10,"coin":500}}
16	{"201307_201308":{"play":9,"coin":0},"201301_201302_201303":{"play":80,"coin":8610},"201304_201305_201306":{"play":5,"coin":0}}
20	{"201307_201308":{"play":34,"coin":10115},"201301_201302_201303":{"play":48,"coin":25671},"201304_201305_201306":{"play":62,"coin":34344}}
25	{"201301_201302_201303":{"play":1,"coin":0}}
46	{"201307_201308":{"play":1,"coin":0},"201304_201305_201306":{"play":23,"coin":0}}
51	{"201307_201308":{"play":1,"coin":0}}
53	{"201307_201308":{"play":3,"coin":0},"201301_201302_201303":{"play":1,"coin":0},"201304_201305_201306":{"play":2,"coin":0}}
117	{"201307_201308":{"play":1,"coin":0},"201301_201302_201303":{"play":12,"coin":160},"201304_201305_201306":{"play":2,"coin":0}}
 
*/
 
select user_id,info['201301_201302_201303'].play, info['201301_201302_201303'].coin from tmp_jin3



-------------------------------------------------------------------------------------
select * from yingzi.seg_rfm_info limit 100;
--user_id	p1	p2	p3	p1_recency	p1_play_days	p1_spend_coin	p2_recency	p2_play_days	p2_spend_coin	p3_recency	p3_play_days	p3_spend_coin	p1p2_play_days	p1p2_spend_coin	p2p3_play_days	p2p3_spend_coin
--1074	R0F0M0	R1F1M1	R0F0M0	notplay	0	0	20130516	1	0	notplay	0	0	1	0	-1	0
--1454	R1F1M1	R4F1M1	R1F1M1	20130107	1	0	20130622	1	0	20130703	1	0	0	0	0	0
-------------------------------------------------------------------------------------

add jar hdfs:///user/hive/lib/jruby.jar;
add jar hdfs:///user/hive/lib/monkey-spanner-dev.jar;
create temporary function exec_jruby as 'spanner.monkey.hive.GenericUDFCallJRubyV2';
 
 
-- setting the number of reduce tasks
set mapred.reduce.tasks=10;

set exec = "def exec(recency,days,coin)
	r=case recency
		when 0..7 then'R5'
		when 7..14 then 'R4'
		when 14..21 then 'R3'
		when 21..30 then 'R2'
		when 30..90 then 'R1'
		when 90..Float::INFINITY then 'R0'
		else 'R999'
	end	
	f=case days
		when 0 then 'F0'
		when 0..3 then 'F1'
		when 3..7  then 'F2'
		when 7..14 then 'F3'
		when 14..25 then 'F4'
		when 25..Float::INFINITY then 'F5'
		#else 'F999'
	end
	m=case coin 
		when 0 then 'M1'
		when 0..500 then 'M2'
		when 500..2000 then 'M3'
		when 2000..10000 then 'M4'
		when 10000..Float::INFINITY then 'M5'
		#else 'M999'
	end	
	r+f+m
end
";
 --p exec(90,10,10)
--select exec_jruby(${hiveconf:exec}, datediff(${hiveconf:ld},concat(substr(last_play,1,4),'-',substr(last_play,5,2),'-',substr(last_play,7,2))), play_days, COALESCE(spend_coin,0) ) as p1 from yingzi.seg_raw where period='201301_201302_201303'
--select user_id, play_days,spend_coin, exec_jruby(${hiveconf:exec}, datediff(${hiveconf:ld},concat(substr(last_play,1,4),'-',substr(last_play,5,2),'-',substr(last_play,7,2))), play_days, COALESCE(spend_coin,0) ) as p1 from yingzi.seg_raw where period='201301_201302_201303' limit 100;

create table yingzi.seg_rfm_info as
select uid.user_id
	,COALESCE(t1.p1,'R0F0M0') as p1
	,COALESCE(t2.p2,'R0F0M0') as p2
	,COALESCE(t3.p3,'R0F0M0') as p3
	,COALESCE(t1.last_play,'notplay') as p1_recency
	,COALESCE(t1.play_days,0) as p1_play_days
	,COALESCE(t1.spend_coin,0) as p1_spend_coin
	,COALESCE(t2.last_play,'notplay') as p2_recency
	,COALESCE(t2.play_days,0) as p2_play_days
	,COALESCE(t2.spend_coin,0) as p2_spend_coin
	,COALESCE(t3.last_play,'notplay') as p3_recency
	,COALESCE(t3.play_days,0) as p3_play_days
	,COALESCE(t3.spend_coin,0) as p3_spend_coin
	,(COALESCE(t2.play_days,0) - COALESCE(t1.play_days,0)) as p1p2_play_days
	,(COALESCE(t2.spend_coin,0) - COALESCE(t1.spend_coin,0)) as p1p2_spend_coin
	,(COALESCE(t3.play_days,0) - COALESCE(t2.play_days,0)) as p2p3_play_days
	,(COALESCE(t3.spend_coin,0) - COALESCE(t2.spend_coin,0)) as p2p3_spend_coin
from ( select distinct user_id from yingzi.seg_raw where period in ('201301_201302_201303','201304_201305_201306','201307_201308') ) uid
left outer join (select user_id,last_play,play_days,spend_coin, exec_jruby(${hiveconf:exec}, datediff('2013-03-31',concat(substr(last_play,1,4),'-',substr(last_play,5,2),'-',substr(last_play,7,2))), play_days, COALESCE(spend_coin,0) ) as p1 from yingzi.seg_raw where period='201301_201302_201303' ) t1 on uid.user_id = t1.user_id
left outer join (select user_id,last_play,play_days,spend_coin, exec_jruby(${hiveconf:exec}, datediff('2013-06-30',concat(substr(last_play,1,4),'-',substr(last_play,5,2),'-',substr(last_play,7,2))), play_days, COALESCE(spend_coin,0) ) as p2 from yingzi.seg_raw where period='201304_201305_201306') t2 on uid.user_id = t2.user_id
left outer join (select user_id,last_play,play_days,spend_coin, exec_jruby(${hiveconf:exec}, datediff('2013-08-31',concat(substr(last_play,1,4),'-',substr(last_play,5,2),'-',substr(last_play,7,2))), play_days, COALESCE(spend_coin,0) ) as p3 from yingzi.seg_raw where period='201307_201308') t3 on uid.user_id = t3.user_id
 


-------------------------------------------------------------------------------------
select * from yingzi.user_application where period = '201301_201302_201303' limit 100;
--user_id	app_playdays	app_spendtimes	app_playdate	                     app_spenddate	     period
--3954	     {58644:1,58768:3}	{58769:5}     {"20130301":1,"20130131":1,"20130306":1}	{"2013-01-22":1} 201301_201302_201303
--3941	       {56359:3}	<null>	      {"20130130":1,"20130131":1,"20130124":1}	 <null>	         201301_201302_201303
-------------------------------------------------------------------------------------

create table yingzi.user_application(
	user_id bigint,
	app_playdays map<bigint,int>,
	app_spendtimes map<bigint,int>,
	app_playdate map<string,int>,
	app_spenddate map<string,int>
) partitioned by (period string);
 
desc yingzi.user_application;
----------
 
--add jar hdfs:///path/to/jruby.jar;
add jar hdfs:///user/hive/lib/jruby.jar;
add jar hdfs:///user/hive/lib/monkey-spanner-dev.jar;
create temporary function map_count as 'spanner.monkey.hive.GenericUDAFMapCounter';
 
--insert into table sandbox.yingzi_user_application partition(period='201301_201302_201303')
insert into table yingzi.user_application partition(period='201304_201305_201306')
--insert into table sandbox.yingzi_user_application partition(period='201307_201308')
select t1.user_id, t1.app_playdays, t2.app_spendtimes, t1.app_playdate, t2.app_spenddate 
from(
	select user_id, map_count(details.application_id) as app_playdays, map_count(distinct dt) as app_playdate
	from default.log_platform_ggp
--	where dt between '20130101' and '20130331' and user_info.region = 'JP' and device_id=2
	where dt between '20130401' and '20130630' and user_info.region = 'JP' and device_id=2
--	where dt between '20130101' and '20130331' and user_info.region = 'JP' and device_id=2
	group by user_id
)t1
left outer join
(
	select user_id, map_count(subtype) as app_spendtimes, map_count(distinct dt) as app_spenddate
	from default.gree_coin
--	where dt between '2013-01-01' and '2013-03-31' and type=-2010 and nondeveloper_coin<0
	where dt between '2013-04-01' and '2013-06-30' and type=-2010 and nondeveloper_coin<0
--	where dt between '2013-01-01' and '2013-03-31' and type=-2010 and nondeveloper_coin<0
	group by user_id
)t2
on (t1.user_id=t2.user_id and t1.user_id !=0)



-------------------------------------------------------------------------------------
select * from yingzi.app_df limit 100;
--app	df
--1	1956856
--101	2
-------------------------------------------------------------------------------------

create table yingzi.app_df as
select details.application_id as app
	, count(distinct user_id) as df 
from default.log_platform_ggp 
where dt is not null and user_info.region = 'JP' and device_id=2
group by details.application_id



-------------------------------------------------------------------------------------
select * from yingzi.seg_rfm_tfidf_app limit 100;
--seg	app	stf	df	tfidf	info	pp
--R1F1M1	96	10,482.00103	810141	33,702.51442	playdays	p1
--R1F1M1	406	456.05183	19500	3,918.35259	playdays	p1
-- info: {playdays, spendtimes}, pp: {p1,p2}
-------------------------------------------------------------------------------------

add jar hdfs:///user/hive/lib/jruby.jar;
add jar hdfs:///user/hive/lib/monkey-spanner-dev.jar;
create temporary function exec_jruby as 'spanner.monkey.hive.GenericUDFCallJRubyV2';
set mapred.reduce.tasks = 10;
 
set exec = '
def exec ( h )
  tfsum = h.values.reduce(:+)
  h.each{|k,v| h[k] = v.to_f/tfsum}
  h
end
';

set period='201301_201302_201303';

create table yingzi.seg_rfm_tfidf_app as
select
t3.seg, t3.app, t3.stf, t2.df, t3.stf*log2(7524104/df) as tfidf,'playdays' as info,'p1' as pp
from yingzi.app_df t2
join
(
   select
   seg, app, sum(day) as stf
   from ( 
     select a.user_id, b.p1 as seg, exec_jruby(Map(1, 1.1), ${hiveconf:exec}, a.app_playdays) as ap
   	from (select * from yingzi.user_application where period=${hiveconf:period} ) a
   	left outer join yingzi.seg_rfm_info b
   	on ( a.user_id = b.user_id)
   	where b.user_id is not null
   ) t1 lateral view explode(ap) pdTable as app, day
   group by seg, app
) t3 
on ( t2.app = t3.app);

insert into table yingzi.seg_rfm_tfidf_app
select
t3.seg, t3.app, t3.stf, t2.df, t3.stf*log2(7524104/df) as tfidf,'spendtimes' as info,'p1' as pp
from yingzi.app_df t2
join
(
   select
   seg, app, sum(day) as stf
   from ( 
     select a.user_id, b.p1 as seg, exec_jruby(Map(1, 1.1), ${hiveconf:exec}, a.app_spendtimes) as ap
   	from (select * from yingzi.user_application where period=${hiveconf:period} and app_spendtimes is not null) a
   	left outer join yingzi.seg_rfm_info b
   	on ( a.user_id = b.user_id)
   	where b.user_id is not null
   ) t1 lateral view explode(ap) pdTable as app, day
   group by seg, app
) t3 
on ( t2.app = t3.app);

set period='201304_201305_201306';

insert into table  yingzi.seg_rfm_tfidf_app
select
t3.seg, t3.app, t3.stf, t2.df, t3.stf*log2(7524104/df) as tfidf,'playdays' as info,'p2' as pp
from yingzi.app_df t2
join
(
   select
   seg, app, sum(day) as stf
   from ( 
     select a.user_id, b.p2 as seg, exec_jruby(Map(1, 1.1), ${hiveconf:exec}, a.app_playdays) as ap
   	from (select * from yingzi.user_application where period=${hiveconf:period} ) a
   	left outer join yingzi.seg_rfm_info b
   	on ( a.user_id = b.user_id)
   	where b.user_id is not null
   ) t1 lateral view explode(ap) pdTable as app, day
   group by seg, app
) t3 
on ( t2.app = t3.app);

insert into table yingzi.seg_rfm_tfidf_app
select
t3.seg, t3.app, t3.stf, t2.df, t3.stf*log2(7524104/df) as tfidf,'spendtimes' as info,'p2' as pp
from yingzi.app_df t2
join
(
   select
   seg, app, sum(day) as stf
   from ( 
     select a.user_id, b.p2 as seg, exec_jruby(Map(1, 1.1), ${hiveconf:exec}, a.app_spendtimes) as ap
   	from (select * from yingzi.user_application where period=${hiveconf:period} and app_spendtimes is not null) a
   	left outer join yingzi.seg_rfm_info b
   	on ( a.user_id = b.user_id)
   	where b.user_id is not null
   ) t1 lateral view explode(ap) pdTable as app, day
   group by seg, app
) t3 
on ( t2.app = t3.app);


/* original code
insert into table yingzi.seg_app_tfidf
select 'F5M1' as class, a1.app, stf, df, stf*log2(7524104/df) as tfidf, '201301_201302_201303' as period
from(
  -- target user group's app_play stf
	select app, sum(day) as stf
	from(
		--TF: select explode(app_playdays) as (app,day)
		--STF: standardize tf for each user
		select explode(exec_jruby(Map(1, 1.1), ${hiveconf:exec}, app_playdays)) as (app,day)
		--select explode(app_playdays) as (app,day)
		from(
			select info.user_id, info.app_playdays from (select * from yingzi.user_application where period='201301_201302_201303') info
			left semi join (select user_id from yingzi.seg_info where p1='F5M1') seg on info.user_id= seg.user_id
		) t
	) t1
	group by app
) a1
join
(
	select app, df from yingzi.app_df
)a2
on a1.app=a2.app
*/
 
--
--- get Top 10 app by segment
--
 
create temporary function p_rank as 'spanner.monkey.hive.PsuedoRank';
 
select * from
(
   select seg, app, p_rank(seg) as rank
   from
   (
      select * from seg_app_tfidf distribute by seg sort by seg, tfidf desc
   ) t1
) t2
where rank <= 10
;
 
 
--- or 
 
create temporary function to_sorted_array as 'spanner.monkey.hive.GenericUDAFToSortedArray';
set top_10 = '
def exec(list)
  list[0..9]
end
';
 
select seg,
  exec_jruby(Array(1), ${hiveconf:top_10}, to_sorted_array(app,-tfidf)) as top10
from seg_app_tfidf
group by seg;
 
-- F2M5 [98,2522,1242,112,53331,1,58309,58737,57737,99]
-- F3M4 [2522,58737,57737,98,1242,1,112,1604,57528,99]
-- F4M3 [58737,98,1,57737,57528,95,2676,1494,1604,1236]
-- F5M2 [1,1236,98,95,1604,57737,1494,58737,2676,3241]
-- F3M5 [1242,98,2522,58737,1,112,57737,2676,99,1604]
-- F4M4 [58737,2522,98,57737,1,57528,1604,95,2676,1494]
-- F5M3 [1,95,98,1494,1236,2676,58737,1604,57737,2620]
-- F4M5 [1242,98,58737,2522,112,1,2676,99,57737,1604]
-- F5M4 [1,98,1494,2676,95,1236,1604,57737,58737,2620]
-- F5M5 [98,1,2676,1494,1604,1236,95,57737,943,2522]
-- F1M1 [57737,98,1604,56359,96,1,95,2676,58737,1494]
-- F1M2 [2522,57737,58737,98,1242,1494,112,1,97,2676]
-- F2M1 [57737,1,1604,98,95,1236,2676,58737,96,56359]
-- F1M3 [2522,112,1242,98,97,57737,99,58737,95,2676]
-- F2M2 [57737,58737,2522,98,1,1604,1494,95,2676,57528]
-- F3M1 [1,57737,98,95,1236,1604,2676,58737,96,3241]
-- F1M4 [2522,98,1242,57737,112,58309,1,58737,99,97]
-- F2M3 [2522,58737,57737,98,95,2676,112,1242,1,99]
-- F3M2 [58737,57737,1,98,57528,95,1604,1494,2676,1236]
-- F4M1 [1,1236,98,95,1604,57737,3241,58737,2676,1494]
-- F1M5 [53331,58309,56536,58623,52772,112,1494,98,3262,57564]
-- F2M4 [2522,98,1242,57737,58737,112,1,1604,99,97]
-- F3M3 [58737,2522,57737,98,1,95,57528,2676,1604,58703]
-- F4M2 [58737,57737,1,98,57528,1236,95,1604,1494,2676]
-- F5M1 [1,95,1236,2676,98,1604,1494,3241,841,57737]-- 


-------------------------------------------------------------------------------------
select * from yingzi.seg_user_feature limit 100
--user_id	p1	p2	p3	p1_recency	p2_recency	p1_play_days	p2_play_days	p1_spend_coin	p2_spend_coin	app_playdays	app_spendtimes	p1_play_app_count	p1_spend_app_count	p1_play_delta	p1_spend_delta
--51	R0F0M0	R0F0M0	R1F1M1	notplay	notplay	0	0	0	0	<null>	<null>	[0,0]	[0,0]	[0]	[0]
--376	R0F0M0	R0F0M0	R2F1M1	notplay	notplay	0	0	0	0	<null>	<null>	[0,0]	[0,0]	[0]	[0]
--2501	R4F5M1	R1F1M1	R5F5M1	20130322	20130425	49	2	0	0	{58764:1,56672:1,58510:1,57227:1,58857:1,1801:2,2676:2,3241:4,52206:2,1604:47}	<null>	[10,62]	[0,0]	[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,4,1,1,1,1,1,1,2,1,2,1,1,1,1,1,1,1,2,9,8,11]	[0]
--2876	R0F0M0	R0F0M0	R3F1M1	notplay	notplay	0	0	0	0	<null>	<null>	[0,0]	[0,0]	[0]	[0]
--2926	R5F5M4	R5F3M1	R5F3M4	20130331	20130625	88	9	5500	0	{57933:88,57856:2}	{57933:11}	[2,90]	[1,11]	[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2]	[2,13,7,2,1,1,15,27]
-------------------------------------------------------------------------------------


--最初に追加 (rubyでrequireを呼び出す時に必要)
add file hdfs:///user/hive/lib/hive-site.xml;
add jar hdfs:///user/hive/lib/jruby.jar;
add jar hdfs:///user/hive/lib/monkey-spanner-dev.jar;
--create temporary function exec_jruby as 'spanner.monkey.hive.GenericUDFCallJRubyV2';
create temporary function exec_jruby as 'spanner.monkey.hive.GenericUDFCallJRubyV3';

set mapred.reduce.tasks = 25;

set rb = '
require "date"
def exec2( days )
     dts = days.map {|d| Date.parse(d).to_time.to_i}
     res = []
     dts.each_with_index {|d,i|
       if i > 0
            res << ((dts[i] - dts[i-1]) / 86400).to_i
       end
     }
     res
end
def exec1( h )
     if h.nil?
            [0,0]
     else
          k = h.count
          v = h.values.reduce(:+)
          [k,v]
     end
end
';

--p exec2(["2013-03-30","2013-04-01","2013-04-20"])
--p exec2(["20130330","20130401","20130420"])
--p exec1({58737=>3,58642=>1,57528=>1,57737=>2,58703=>3,58748=>2})


use yingzi;
show tables;

create table yingzi.seg_user_feature as
select user_id, p1, p2, p3, p1_recency, p2_recency, p1_play_days, p2_play_days, p1_spend_coin, p2_spend_coin
     , app_playdays
     , app_spendtimes
     , exec_jruby(Array(1),Array(${hiveconf:rb},'exec1'),app_playdays) as p1_play_app_count
     , exec_jruby(Array(1),Array(${hiveconf:rb},'exec1'),app_spendtimes) as p1_spend_app_count   
     , case
          when app_playdate is null then array(cast ( 0 as bigint ))
          else exec_jruby(Array(1),Array(${hiveconf:rb},'exec2'),sort_array(map_keys(app_playdate))) end as p1_play_delta    
	, case
         when app_spenddate is null then array(cast ( 0 as bigint ))
         else exec_jruby(Array(1),Array(${hiveconf:rb},'exec2'),sort_array(map_keys(app_spenddate))) end as p1_spend_delta  
from
(
     select a.*, b.app_playdays, b.app_spendtimes, b.app_playdate, b.app_spenddate from
     (select * from yingzi.seg_rfm_info) a
     --(select * from yingzi.seg_rfm_info where p1='R5F5M5' and p2='R1F1M1') a
     left outer join (select * from yingzi.user_application where period='201301_201302_201303') b on a.user_id=b.user_id
     distribute by a.user_id
) t1;

