with 
    row_nums as (
        select *, 
            row_number() over (partition by game_id, player_id) as rn 
        from game_details
    ),

    deduped as (
        select * 
        from row_nums 
        where rn = 1
    )

select * 
from deduped
;