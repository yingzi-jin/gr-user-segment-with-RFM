/****** SSMS からの SelectTopNRows コマンドのスクリプト  ******/

select distinct category from [summary].[dbo].[login_all_201307]

/** ログインと消費テーブル**/
select user_id, count(distinct(date)) login_day
from [summary].[dbo].[login_all_201304]
where [category] = 2 
group by user_id

select COUNT(distinct user_id) as SUU 
	,SUM([coin_nondeveloper]) spend_coin
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201304] 


/************************

  セグメンテーションとその間のユーザ移動状況
  
*************************/

/* ログイン-消費ランクテーブル
 P1:1~3月 [users].[dbo].[yingzi_seg_P1_LC]
 P2:4~6月 [users].[dbo].[yingzi_seg_P2_LC]
 p3:7~8月 [users].[dbo].[yingzi_seg_P3_LC]
 */

select a.user_id
	,case
		when a.login_day>0 and a.login_day<4 then 'L1' --light user (1~3)
		when a.login_day>=4 and a.login_day<8 then 'L2' -- low middle user (4~7)
		when a.login_day>=8 and a.login_day<15 then 'L3' -- high middle user (8~14)
		when a.login_day>=15 and a.login_day<25 then 'L4' -- low core user(15~24)
		when a.login_day>=25 then 'L5' -- high core user(25~)
	end as L
	,case
		when b.spend_coin is null then 'C1' -- white user(0)
		when b.spend_coin>0 and b.spend_coin<500 then 'C2' -- bronze user (1~500)
		when b.spend_coin>=500 and b.spend_coin<2000 then 'C3' -- silver user (500~2000)
		when b.spend_coin>=2000 and b.spend_coin<10000 then 'C4' -- gold user (2000~10000)
		when b.spend_coin>=10000 then 'C5' -- platinum user (10000~)
	end as C
	,a.login_day
	,isnull(b.spend_coin,0) as spend_coin	
	,1 as uu
--into [users].[dbo].[yingzi_seg_P1_LC]
--into [users].[dbo].[yingzi_seg_P2_LC]
into [users].[dbo].[yingzi_seg_P3_LC]
from 
(
	select user_id, ceiling(count(distinct(date))*1.0/3) as login_day
	from (
		/*
		select * from [summary].[dbo].[login_all_201301]
		union all
		select * from [summary].[dbo].[login_all_201302]
		union all
		select * from [summary].[dbo].[login_all_201303]
		*/
		/*
		select * from [summary].[dbo].[login_all_201304]
		union all
		select * from [summary].[dbo].[login_all_201305]
		union all
		select * from [summary].[dbo].[login_all_201306]
		*/
		select * from [summary].[dbo].[login_all_201307]
		union all
		select * from [summary].[dbo].[login_all_201308]
	)t
	where [category] = 2 
	group by user_id
) a -- login
left join 
(
	select user_id, ceiling(sum([coin_nondeveloper])*1.0/3) as spend_coin, ceiling(count(distinct(date))*1.0/3) as spend_day
	from (
		/*
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201301]
		union all
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201302]
		union all
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201303]
		*/
		/*
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201304]
		union all
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201305]
		union all
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201306]	
		*/
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201307]
		union all
		select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201308]			
	)t
	where region='JP' and type in(-2010)
	group by user_id
) b -- spend
on a.user_id=b.user_id
--(8215246 行処理されました) 03:30
select TOP 10 * from [users].[dbo].[yingzi_seg_P1_LC]
select TOP 10 * from [users].[dbo].[yingzi_seg_P2_LC]


/*P1->P2移動*/
drop table [users].[dbo].[yingzi_seg_P1toP2_LC]
select uuall.user_id
	,isnull(p1.L,'L0')+'-'+isnull(P1.C,'C0') as p1
	,isnull(p2.L,'L0')+'-'+isnull(P2.C,'C0') as p2
	,(isnull(p2.login_day,0) - isnull(p1.login_day,0)) as delta_login_day
	,(isnull(p2.spend_coin,0) - isnull(p1.spend_coin,0)) as delta_spend_coin
	,1 as uu
into [users].[dbo].[yingzi_seg_P1toP2_LC]
from (
	select distinct user_id
	from (
		select * from [users].[dbo].[yingzi_seg_P1_LC]
		union all
		select * from [users].[dbo].[yingzi_seg_P2_LC]
	) t
) uuall	
left join [users].[dbo].[yingzi_seg_P1_LC] p1
on uuall.user_id=p1.user_id
left join [users].[dbo].[yingzi_seg_P2_LC] p2
on uuall.user_id=p2.user_id
--(11608885 行処理されました)


select TOP 10 * from [users].[dbo].[yingzi_seg_P1_LC]

/****
重要セグメント調査
*****/

select TOP 1000 * from [users].[dbo].[yingzi_seg_P1toP2_LC]

-- セグメント間移動
select TOP 5
	case when p1='L0-C0' then 'New->' else p1 end as p1
	,case when p2='L0-C0' then '->Churn' else p2 end as p2
	,count(*) as moveUU
	,SUM(delta_login_day) as deltaLOGIN
	,SUM(delta_spend_coin) as deltaCOIN
	,(SUM(delta_spend_coin)* SUM(delta_login_day)*1.0) /(CAST(count(*) as bigint)* count(*) ) as L_C_perUser
	,(LOG(ABS(SUM(delta_spend_coin)+1))* SUM(delta_login_day)*1.0) /count(*) as L_logC_perUser
from [users].[dbo].[yingzi_seg_P1toP2_LC]
group by p1,p2
--order by moveUU desc
--order by deltaLOGIN desc
--order by deltaCOIN desc
--order by L_C_perUser desc
order by L_logC_perUser desc
select TOP 5
	case when p1='L0-C0' then 'New->' else p1 end as p1
	,case when p2='L0-C0' then '->Churn' else p2 end as p2
	,count(*) as moveUU
	,SUM(delta_login_day) as deltaLOGIN
	,SUM(delta_spend_coin) as deltaCOIN
	,(SUM(delta_spend_coin)* SUM(delta_login_day)*1.0) /(CAST(count(*) as bigint)* count(*) ) as L_C_perUser
	,(LOG(ABS(SUM(delta_spend_coin)+1))* SUM(delta_login_day)*1.0) /count(*) as L_logC_perUser
from [users].[dbo].[yingzi_seg_P1toP2_LC]
group by p1,p2
--order by moveUU
--order by deltaLOGIN 
--order by deltaCOIN 
--order by L_C_perUser
order by L_logC_perUser


/**重要セグメントユーザの調査*/

select * from [users].[dbo].[yingzi_seg_P1toP2_LC]
where p1='L0-C0' and p2='L5-C5'


select * from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201304] 






















/************************

  新密度<->消費セグメンテーション
  
*************************/  

-- 消費属性
select user_id
	,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
into #spend
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201304] 
where region='JP' and type in(-2010)
group by user_id
--(772829 行処理されました)
-- select TOP 10 * from #spend
-- drop table #spend2
select user_id
	,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
into #spend2
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201305] 
where region='JP' and type in(-2010)
group by user_id
--(736438 行処理されました)
select user_id
	,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
into #spend3
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201306] 
where region='JP' and type in(-2010)
group by user_id
--(733458 行処理されました)

select MAX(spend_coin) as max_spend_coin,MIN(spend_coin) as min_spend_coin,AVG(spend_coin) as mean_spend_coin,
	MAX(spend_time) as max_spend_time,MIN(spend_time) as min_spend_time,AVG(spend_time) as mean_spend_time
from #spend
--max_spend_coin	min_spend_coin	mean_spend_coin	max_spend_time	min_spend_time	mean_spend_time
--1202646	1	7900	1799	1	13

-- 各層ごとの消費UUとコイン
declare @X int =13
declare @Y int = 7900
select class,COUNT(*) as uu,SUM(spend_coin) as coin
from(
		select *,
			case
				when spend_time>@X and spend_coin>@Y then 'C1'
				when spend_time<=@X and spend_coin>@Y then 'C2'
				when spend_time>@X and spend_coin<=@Y then 'C3'
				when spend_time<=@X and spend_coin<=@Y then 'C4'
			end as class
		from #spend3
)t
group by class

-- 各層間の移動UUとデルタコイン	
declare @X int =13
declare @Y int = 7900
--select class,COUNT(*) as uu,SUM(spend_coin) as coin
select class_from, class_to, count(*) as deltaUU, sum(spend_coin_2-spend_coin_1) as deltaCoin
from
(
	select t1.user_id
		,t1.spend_coin as spend_coin_1
		,t1.spend_time as spend_time_1
		,isnull(t2.spend_coin,0) as spend_coin_2
		,isnull(t2.spend_time,0) as spend_time_2
		,t1.class as class_from
		,isnull(t2.class,'notplay') as class_to
	from(
		select *,
			case
				when spend_time>@X and spend_coin>@Y then 'C1'
				when spend_time<=@X and spend_coin>@Y then 'C2'
				when spend_time>@X and spend_coin<=@Y then 'C3'
				when spend_time<=@X and spend_coin<=@Y then 'C4'
			end as class
		from #spend2
	)t1
	left outer join
	(
		select *,
			case
				when spend_time>@X and spend_coin>@Y then 'C1'
				when spend_time<=@X and spend_coin>@Y then 'C2'
				when spend_time>@X and spend_coin<=@Y then 'C3'
				when spend_time<=@X and spend_coin<=@Y then 'C4'
			end as class
		from #spend3	
	)t2
	on t1.user_id=t2.user_id
)t
group by class_from,class_to
order by class_to,class_from

-- 4~6月の各層の固定ユーザ

--select class,COUNT(*) as uu,SUM(spend_coin) as coin	
declare @X int =13
declare @Y int = 7900
select flag,class1,COUNT(*) as uu 
from(
	select *,
		case
			when class1=class2 and class1=class3 then 'kotei'
			when class2='no' and class3='no' then 'churn'
			else 'move'
		end as flag
	from
	(

		select t1.user_id
			,t1.spend_coin as spend_coin_1
			,t1.spend_time as spend_time_1
			,isnull(t2.spend_coin,0) as spend_coin_2
			,isnull(t2.spend_time,0) as spend_time_2
			,isnull(t3.spend_coin,0) as spend_coin_3
			,isnull(t3.spend_time,0) as spend_time_3		
			,t1.class as class1
			,isnull(t2.class,'no') as class2
			,isnull(t3.class,'no') as class3
		from(
			select *,
				case
					when spend_time>@X and spend_coin>@Y then 'C1'
					when spend_time<=@X and spend_coin>@Y then 'C2'
					when spend_time>@X and spend_coin<=@Y then 'C3'
					when spend_time<=@X and spend_coin<=@Y then 'C4'
				end as class
			from #spend
		)t1
		left outer join
		(
			select *,
				case
					when spend_time>@X and spend_coin>@Y then 'C1'
					when spend_time<=@X and spend_coin>@Y then 'C2'
					when spend_time>@X and spend_coin<=@Y then 'C3'
					when spend_time<=@X and spend_coin<=@Y then 'C4'
				end as class
			from #spend2	
		)t2
		on t1.user_id=t2.user_id
		left outer join
		(
			select *,
				case
					when spend_time>@X and spend_coin>@Y then 'C1'
					when spend_time<=@X and spend_coin>@Y then 'C2'
					when spend_time>@X and spend_coin<=@Y then 'C3'
					when spend_time<=@X and spend_coin<=@Y then 'C4'
				end as class
			from #spend3	
		)t3
		on t1.user_id = t3.user_id
	)t4
)t5
group by flag,class1

/************************

  RFC セグメンテーション
  
*************************/

--半年の消費データ
drop table #spend1
drop table #spend2
drop table #spend3
drop table #spend4
drop table #spend5
drop table #spend6

select user_id,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
	,MAX(CAST(date as date)) last_day
into #spend1
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201301] 
where region='JP' and type in(-2010)
group by user_id
select user_id,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
	,MAX(CAST(date as date)) last_day
into #spend2
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201302] 
where region='JP' and type in(-2010)
group by user_id
select user_id,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
	,MAX(CAST(date as date)) last_day
into #spend3
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201303] 
where region='JP' and type in(-2010)
group by user_id
select user_id,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
	,MAX(CAST(date as date)) last_day
into #spend4
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201304] 
where region='JP' and type in(-2010)
group by user_id
select user_id,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
	,MAX(CAST(date as date)) last_day
into #spend5
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201305] 
where region='JP' and type in(-2010)
group by user_id
select user_id,SUM([coin_nondeveloper]) spend_coin
	,COUNT(distinct(date)) spend_day
	,COUNT(*) spend_time
	,MAX(CAST(date as date)) last_day	
into #spend6
from [mitsuda_analytics].[dbo].[log_platform_ggp_spend_spend_201306] 
where region='JP' and type in(-2010)
group by user_id

drop table #uuall
select user_id, MAX(last_day)  as recency
into #uuall
from(
	select user_id,last_day from #spend1 
	union all select user_id,last_day from #spend2 
	union all select user_id,last_day from #spend3 
	union all select user_id,last_day from #spend4 
	union all select user_id,last_day from #spend5 
	union all select user_id,last_day from #spend6
)t
group by t.user_id
--(1693874 行処理されました)

drop table [users].[dbo].[yingzi_tmp_spend]	
select uuall.user_id
	,uuall.recency
	,isnull(a1.spend_coin,0) as spend_coin_1
	,isnull(a1.spend_day,0) as spend_day_1
	,isnull(a1.spend_time,0) as spend_time_1
	,isnull(a2.spend_coin,0) as spend_coin_2
	,isnull(a2.spend_day,0) as spend_day_2
	,isnull(a2.spend_time,0) as spend_time_2
	,isnull(a3.spend_coin,0) as spend_coin_3
	,isnull(a3.spend_day,0) as spend_day_3
	,isnull(a3.spend_time,0) as spend_time_3
	,isnull(a4.spend_coin,0) as spend_coin_4
	,isnull(a4.spend_day,0) as spend_day_4
	,isnull(a4.spend_time,0) as spend_time_4
	,isnull(a5.spend_coin,0) as spend_coin_5
	,isnull(a5.spend_day,0) as spend_day_5
	,isnull(a5.spend_time,0) as spend_time_5
	,isnull(a6.spend_coin,0) as spend_coin_6
	,isnull(a6.spend_day,0) as spend_day_6
	,isnull(a6.spend_time,0) as spend_time_6
into [users].[dbo].[yingzi_tmp_spend]	 
from #uuall uuall
left join #spend1 a1 on uuall.user_id = a1.user_id
left join #spend2 a2 on uuall.user_id = a2.user_id
left join #spend3 a3 on uuall.user_id = a3.user_id
left join #spend4 a4 on uuall.user_id = a4.user_id
left join #spend5 a5 on uuall.user_id = a5.user_id
left join #spend6 a6 on uuall.user_id = a6.user_id

select TOP 100 * from  [users].[dbo].[yingzi_tmp_spend]	

--select max(frequency),MIN(frequency),AVG(frequency),MAX(monetary),MIN(monetary),AVG(monetary)
--8532	1	39	8006290	1	23314

/* 
--select * from #user_RFM 
select TOP 100 * from [users].[dbo].[yingzi_tmp_user_RFM]
*/
declare @R1 date= cast('2013-03-01' as date)
declare @R2 date= cast('2013-04-01' as date)
declare @R3 date= cast('2013-05-01' as date)
declare @R4 date= cast('2013-06-01' as date)
declare @F1 int=10
declare @F2 int=30
declare @F3 int=80
declare @F4 int=150
declare @M1 int=500
declare @M2 int=2500
declare @M3 int=10000
declare @M4 int=50000
select t.user_id
	,case
		when recency<@R1 then 1
		when recency>=@R1 and  recency < @R2 then 2
		when recency>=@R2 and  recency< @R3 then 3
		when recency>=@R3 and  recency< @R4 then 4
		when recency>=@R4 then 5
	end as R
	,case
		when frequency < @F1 then 1
		when frequency >= @F1 and frequency <@F2  then 2
		when frequency >= @F2 and frequency <@F3  then 3
		when frequency >= @F3 and frequency <@F4  then 4
		when frequency >= @F4  then 5
	end as F
	,case
		when monetary<@M1 then 1
		when monetary>=@M1 and  monetary< @M2 then 2
		when monetary>=@M2 and  monetary< @M3 then 3
		when monetary>=@M3 and  monetary< @M4 then 4
		when monetary>=@M4 then 5
	end as M
	,t.recency
	,t.frequency
	,t.monetary
--into #user_RFM
into [users].[dbo].[yingzi_tmp_user_RFM]
from(
	select user_id
		,recency
		,(spend_time_1+spend_time_2+spend_time_3+spend_time_4+spend_time_5+spend_time_6) as frequency
		,(spend_coin_1+spend_coin_2+spend_coin_3+spend_coin_4+spend_coin_5+spend_coin_6) as  monetary
	from [users].[dbo].[yingzi_tmp_spend]
)t	

/* R, F, MのUU分布*/
select recency, COUNT(*) as UU from [users].[dbo].[yingzi_tmp_user_RFM]
group by recency
order by recency


/* R, F, Mのの1~5ランク別UU*/
select M, COUNT(*) as UU from [users].[dbo].[yingzi_tmp_user_RFM]
group by M
order by M


/* 
select * from #count_RFM 
*/
select
	R,F,M 
	,COUNT(*) as UU, SUM(monetary) as COIN, cast(SUM(monetary)*1.0/COUNT(*) as int) as ARPPU
	,(16-(R+F+M)) as finalRank
into #count_RFM
from #user_RFM
group by R, F, M
order by R desc, F desc, M desc



-- finalRank層別UU, COIN, ARPPU	
select 
	finalRank, SUM(UU) as UU, SUM(COIN) as COIN, SUM(COIN)*1.0/SUM(UU) as ARPPU
from #count_RFM 
group by finalRank
order by finalRank


-- R/F 二次元 (常連、新規、離反）
select R, F, SUM(UU) as UU, SUM(COIN) as COIN, SUM(COIN)*1.0/SUM(UU) as ARPPU
from #count_RFM
group by R,F
order by F desc, R desc

-- M/F 二次元（親密性と消費）
select M, F, SUM(UU) as UU, SUM(COIN) as COIN, SUM(COIN)*1.0/SUM(UU) as ARPPU
from #count_RFM
group by M,F
order by F desc, M desc

-- KNN sampling
select  TOP 500000 
	datediff(d,CAST('2013-01-01' as date),recency) as recency
	,frequency
	,monetary
	,cast(R as varchar(1))+'_'+cast(F as varchar(1))+'_'+CAST(M as varchar(1)) as RFM from [users].[dbo].[yingzi_tmp_user_RFM]
order by NEWID()


/*back up*/
--group by class

drop table [users].[dbo].[yingzi_userseg]
select a.user_id,a.login_day as loginday,
	isnull(b.spend_all,0) as spendcoin,
	isnull(c.login_day,0) as loginday2,
	isnull(d.spend_all,0) as spendcoin2
into [users].[dbo].[yingzi_userseg]
from #login a
left join #spend b
on a.user_id=b.user_id
left join #login2 c
on a.user_id=c.user_id
left join #spend2 d
on a.user_id=d.user_id
select top 100 * from [users].[dbo].[yingzi_userseg]



-- 消費ユーザだけ
select a.*,
	b.login_day,
	isnull(c.login_day,0) as loginday2,
	isnull(d.spend_all,0) as spend_all2,
	isnull(d.spend_day,0) as spend_day2
into #seg  
from #spend a
left join #login b
on a.user_id=b.user_id
left join #login2 c
on a.user_id=c.user_id
left join #spend2 d
on a.user_id=d.user_id 

select  * from #seg



select COUNT(*),COUNT(distinct user_id),SUM(churn),SUM(spend_flag) from [users].[dbo].[yingzi_userseg]

select TOP 100000 * into [users].[dbo].[yingzi_userseg_samp] 

