-- Lab 1 
-- fact day 1 

select
	game_id, team_id, player_id, count(1)
from game_details
group by 1, 2, 3
HAVING count(1) > 1


INSERT into fct_game_details
with deduped as ( 
	select 
		g.game_date_est,
		g.season,
		g.home_team_id, 
		g.visitor_team_id,
		gd.*,  
		row_number() over(partition by gd.game_id, team_id, player_id order by g.game_date_est) as row_num
	from game_details gd
	join games g on gd.game_id = g.game_id
)


select 
	game_date_est as dim_game_date, 
	season as dim_season, 
	team_id as dim_team,
    player_id as dim_player_id,
    player_name as dim_player_name,
	start_position as dim_start_position, 
	team_id = home_team_id as dim_is_playing_at_home, 	
	COALESCE(POSITION('DNP' in comment),0) > 0 as dim_did_not_play,
	COALESCE(POSITION('DND' in comment),0) > 0 as dim_did_not_dress, 
	COALESCE(POSITION('NWT' in comment),0) > 0 as dim_not_with_team, 
	CAST(SPLIT_PART(min, ':', 1) as REAL) + 
	CAST(SPLIT_PART(min ,':', 2) as REAL)/60 as m_minutes, 
	fgm as m_fgm,
	fga as m_fga, 
	fg3m as m_fg3m, 
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb, 
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnovers,
	pf as m_pf,
	pts as m_pts, 
	plus_minus as m_plus_minus
from deduped
where row_num = 1 

CREATE TABLE fct_game_details(
	dim_game_date DATE, 
	dim_season INTEGER, 
	dim_team_id INTEGER, 
	dim_player_id INTEGER, 
	dim_player_name TEXT, 
	dim_start_position TEXT, 
	dim_is_playing_at_home BOOLEAN, 
	dim_did_not_play BOOLEAN, 
	dim_did_not_dress BOOLEAN, 
	dim_not_with_team BOOLEAN, 
	m_minutes REAL, 
	m_fgm INTEGER, 
	m_fga INTEGER, 
	m_fg3m INTEGER, 
	m_fg3a INTEGER, 
	m_ftm INTEGER, 
	m_fta INTEGER, 
	m_oreb INTEGER, 
	m_dreb INTEGER, 
	m_reb INTEGER, 
	m_ast INTEGER, 
	m_stl INTEGER,
	m_blk INTEGER, 
	m_turnover INTEGER, 
	m_pf INTEGER, 
	m_pts INTEGER, 
	m_plus_minus INTEGER,
	PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
)

select * from fct_game_details

-- Lab 2 
-- fact lab 2 

create table users_cumulated (

	user_id text, 
	-- The list of dates in the past when the user was active
	dates_active DATE[], 
	-- Current date for the user 
	date DATE, 
	PRIMARY KEY(user_id, date)
)


INSERT INTO users_cumulated 
with yesterday as (
	select 
		* 
	from users_cumulated 
	where date = DATE('2023-01-14')
),
today as (
	select 
		CAST(user_id as TEXT),
		DATE(CAST(event_time as timestamp)) as date_active
	from events 
	where DATE(CAST(event_time as timestamp)) = DATE('2023-01-15') and user_id is not null 
	GROUP BY user_id, DATE(CAST(event_time as timestamp))
)

select 
	COALESCE(t.user_id, y.user_id) as user_id, 
	CASE WHEN y.dates_active is null
	THEN array[t.date_active]
	WHEN t.date_active is null 
	then y.dates_active
	else ARRAY[t.date_active] || y.dates_active
	end as dates_active, 
	COALESCE(t.date_active, y.date + INTERVAL '1 day') as date  
from today t full outer join yesterday y 
on t.user_id = y.user_id 



with users as ( 
	select * from users_cumulated 
	where date = '2023-01-15'
),
series as (
	select * 
	from generate_series(DATE('2023-01-01'), DATE('2023-01-15'), INTERVAL '1 day') as series_date
),

place_holder_ints as (
select  
	CASE WHEN dates_active @>  ARRAY[DATE(series_date)]
	THEN CAST(POW(2, 16 - (date - DATE(series_date))) as bigint)
	ELSE 0 
	END as place_holder_int_value, 
	*
from users CROSS JOIN series
)


select 
	user_id,
	CAST(CAST(sum(place_holder_int_value) as BIGINT) as BIT(16)),
	BIT_COUNT(CAST(CAST(sum(place_holder_int_value) as BIGINT) as BIT(16))) > 0 as dim_is_monthly_active,
	BIT_COUNT(CAST('1111111000000000' as BIT(16)) & CAST(CAST(sum(place_holder_int_value) as BIGINT) as BIT(16))) > 0 as dim_is_weekly_active
from place_holder_ints
GROUP BY user_id 




