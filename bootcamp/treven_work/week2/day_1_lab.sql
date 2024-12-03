create table fct_game_details (
    dim_game_date date,
    dim_season int,
    dim_team_id int,
    dim_player_id int,
    dim_player_name text,
    dim_start_position text,
    dim_is_playing_at_home boolean,
    dim_did_not_play boolean,
    dim_did_not_dress boolean,
    dim_not_with_team boolean,
    m_minutes real,
    m_fgm int,
    m_fga int,
    m_fg3m int,
    m_fg3a int,
    m_ftm int,
    m_fta int,
    m_oreb int,
    m_dreb int,
    m_reb int,
    m_ast int,
    m_stl int,
    m_blk int,
    m_turnovers int,
    m_pf int,
    m_pts int,
    m_plus_minus int,
    primary key(dim_game_date, dim_team_id, dim_player_id)
)
;

select * from fct_game_details;






insert into fct_game_details
with 
    rownums as (
    select g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        row_number() over (partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
    from game_details gd 
    inner join games g on gd.game_id = g.game_id
    
    ),

    deduped as ( 
        select * 
        from rownums
        where row_num = 1
    )

select 
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id as dim_is_playing_at_home,    
    coalesce(position('DNP' in comment), 0) > 0 as dim_did_not_play,
    coalesce(position('DND' in comment), 0) > 0 as dim_did_not_dress,
    coalesce(position('NWT' in comment), 0) > 0 as dim_not_with_team,
    cast(split_part(min, ':', 1) as real) +
    (cast(split_part(min, ':', 2) as real) / 60) as m_minutes,    
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
;


select 
    dim_player_name,
    count(1) as num_games,
    count(case when dim_not_with_team then 1 end) as bailed_num,
    cast(count(case when dim_not_with_team then 1 end) as real) / count(1) as bailed_pct
from fct_game_details
group by 1
order by 4 desc
;

