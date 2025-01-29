-- Lab 1 

SELECT * FROM player_seasons
ORDER BY player_name;

CREATE TYPE season_stats AS (
	SEASON INTEGER,
	GP INTEGER,
	PTS REAL,
	REB REAL,
	AST REAL
)

DROP TABLE players; 

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad'); 

CREATE TABLE players (
	player_name TEXT, 
	height TEXT, 
	college TEXT, 
	country TEXT, 
	draft_year TEXT, 
	draft_round TEXT, 
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class, 
	years_since_last_season INTEGER, 
	current_season INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY(player_name, current_season)
)

INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

	select * from players 


create table players_scd ( 
	player_name TEXT, 
	scoring_class scoring_class, 
	is_active BOOLEAN, 
	start_season INTEGER, 
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
);

-- Lab 2 

INSERT into players_scd 
WITH with_previous as ( 
select 
	player_name, 
	scoring_class, 
	current_season, 
	is_active, 
	LAG(scoring_class, 1) OVER (partition by player_name ORDER by current_season) as previous_scoring_class,
	LAG(is_active, 1) OVER (partition by player_name ORDER by current_season) as previous_is_active 
from players
where current_season <= 2021),

with_indicators as ( 
SELECT *, 
	CASE WHEN scoring_class <> previous_scoring_class THEN 1
	WHEN is_active <> previous_is_active THEN 1
	ELSE 0 
	END AS change_indicator
from with_previous ),

with_streaks as ( 
SELECT *, 
sum(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) as streak_identifier
FROM with_indicators
)

select 
	player_name, 
	scoring_class,
	is_active, 
	MIN(current_season) as start_season,
	MAX(current_season) as end_season, 
	2021 as current_season 
from with_streaks
group by player_name, streak_identifier, is_active, scoring_class
order by player_name 

select * from players_scd 


create type scd_type as (

	scoring_class scoring_class, 
	is_active BOOLEAN, 
	start_season INTEGER, 
	end_season INTEGER
)


With last_season_scd as ( 
	select * from players_scd 
	where current_season = 2021
	and end_season = 2021
), 

historical_scd as ( 
	select * from players_scd
	where current_season = 2021
	and end_season < 2021
), 

this_season_data as ( 
	select * from players 
	where current_season = 2021 
),
unchanged_records as ( 
	select ts.player_name, 
		ts.scoring_class, ts.is_active, 
		ls.start_season, ts.current_season as end_season 
	from this_season_data ts 
	JOIN last_season_scd ls 
	on ls.player_name = ts.player_name 
		where ts.scoring_class = ls.scoring_class 
		and ts.is_active = ls.is_active 
),
changed_records as (
	select ts.player_name,
		UNNEST(ARRAY[
			row(
			ls.scoring_class, 
			ls.is_active, 
			ls.start_season, 
			ls.end_season
			) :: scd_type, 
			row(
			ts.scoring_class, 
			ts.is_active, 
			ts.current_season, 
			ts.current_season
			)::scd_type
		])
	from this_season_data ts 
	 left join last_season_scd ls 
	on ls.player_name = ts.player_name 
		where (ts.scoring_class <> ls.scoring_class 
		or ts.is_active <> ls.is_active)
)

select * from unchanged_records 


-- Lab 3 
-- Building a graph data model 
CREATE type vertex_type
AS ENUM ('player','team','game'); 


CREATE table vertices (
	identifier TEXT, 
	type vertex_type, 
	properties JSON, 
	PRIMARY KEY(identifier, type) 
);

CREATE type edge_type
	AS ENUM ('plays_against','shares_team','plays_in', 'plays_on');


CREATE table edges (
	subject_identifier text, 
	subject_type vertex_type, 
	object_identifier text, 
	object_type vertex_type,
	edge_type edge_type, 
	properties JSON,
	PRIMARY KEY(subject_identifier, subject_type, object_identifier, object_type, edge_type)
)

-- vertices *******

INSERT into vertices
select 
	game_id as identifier, 
	'game'::vertex_type as type, 
	json_build_object(
		'pts_home', pts_home, 
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 then home_team_id else visitor_team_id end 
	) as properties 
from games

INSERT into vertices
With players_agg as (
select  
	player_id as identifier,
	MAX(player_name) as player_name,
	count(1) as number_of_games,
	sum(pts) as total_points, 
	ARRAY_AGG(DISTINCT team_id) as teams
from game_details 
group by player_id 
)

SELECT 
	identifier, 
	'player'::vertex_type, 
	json_build_object('player_name',player_name,'number_of_games',number_of_games,'total_points',total_points,'teams',teams )
FROM players_agg


INSERT INTO vertices
with teams_deduped as (
	select *, row_number() OVER(Partition by team_id) as row_num 
	from teams 
)
SELECT 
	team_id as identifier, 
	'team'::vertex_type as type, 
	json_build_object(
	'abbreviation',abbreviation, 'nickname',nickname,'city',city,'arena', arena, 'year_founded',yearfounded
	)
FROM teams_deduped 
where row_num =1 

select type, count(1) 
from vertices
group by 1 

-- edges ******* 

insert into edges 
with edge_deduped as (
	select *, row_number() over(partition by player_id, game_id) as row_num
	from game_details
)


select 	
	player_id as subject_identifier,
	'player'::vertex_type as subject_type, 
	game_id as object_identifier, 
	'game'::vertex_type as object_type, 
	'plays_in'::edge_type as edge_type,
	json_build_object(
		'start_position',start_position,'pts',pts,'team_id',team_id,'team_abbreviation',team_abbreviation
	) as properties
from edge_deduped
where row_num = 1


--- check out one of the edges 
select v.properties->>'player_name', MAX(CAST(e.properties->>'pts' as integer)) as tot from vertices v join edges e 
	on e.subject_identifier = v.identifier 
	and e.subject_type = v.type 
group by 1 
order by 2 desc

insert into edges
with edge_deduped as (
	select *, row_number() over(partition by player_id, game_id) as row_num
	from game_details
),
filtered as (
	select * from edge_deduped 
	where row_num = 1 
),

step as (
select 
	f1.player_id as subject_player_id, 
	f2.player_id as obj_player_id, 
	CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type ELSE 'plays_against'::edge_type end as edge_type,
	COUNT(1) as num_games,
	max(f1.player_name) as subject_player_name, 
	max(f2.player_name) as obj_player_name, 
	sum(f1.pts) as subject_points,
	sum(f2.pts) as obj_points 
from filtered f1 
join filtered f2 
on f1.game_id = f2.game_id 
and f1.player_name <> f2.player_name
where f1.player_id > f2.player_id
group by 1,2,3
)

select 
	subject_player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	obj_player_id as object_identifier,
	'player'::vertex_type as object_type,
	edge_type as edge_type, 
	json_build_object(
		'num_games',num_games,'subject_points',subject_points,'obj_points',obj_points 
	)
from step 


select 
	v.properties->>'player_name', 
	e.object_identifier,
	CAST(v.properties->>'number_of_games' as REAL)/ 
	CASE WHEN CAST(v.properties->>'total_points' as REAL) = 0 THEN 1 ELSE 
		CAST(v.properties->>'total_points' as REAL) END,
	e.properties->>'subject_points',
	e.properties->>'num_games'
	
from vertices v join edges e 
	on v.identifier = e.subject_identifier
	and v.type = e.subject_type
where e.object_type = 'player'::vertex_type

