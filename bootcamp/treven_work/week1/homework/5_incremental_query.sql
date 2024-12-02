create type actor_scd_type as (
    quality_class quality_class,
    is_active boolean,
    start_date int,
    end_date int
)
;

with 
    last_year_scd as (
        select *
        from actors_history_scd
        where year = 2020
        and end_date = 2020
    ),

    historical_scd as (
        select actor,
            quality_class,
            is_active,
            start_date,
            end_date
        from actors_history_scd
        where year = 2020
        and end_date < 2020
    ),

    this_year_data as (
        select *
        from actors
        where year = 2021
    ),

    unchanged_records as (
        select ts.actor,
            ts.quality_class,
            ts.is_active,
            ls.start_date,
            ls.year as end_season
        from this_year_data ts 
        inner join last_year_scd ls on ts.actor = ls.actor
        where ts.quality_class = ls.quality_class
        and ts.is_active = ls.is_active
    ),

    changed_records as (
        select ts.actor,            
            unnest(array[
                row(
                    ls.quality_class,
                    ls.is_active,
                    ls.start_date,
                    ls.end_date
                )::actor_scd_type,
                row(
                    ts.quality_class,
                    ts.is_active,
                    ts.year,
                    ts.year
                )::actor_scd_type
            ]) as records
        from this_year_data ts 
        left join last_year_scd ls on ts.actor = ls.actor
        where 
            (
                ts.quality_class <> ls.quality_class
                or ts.is_active <> ls.is_active
            )
          
    ),

    unnested_changed_records as (
        select actor,
            (records::actor_scd_type).*
        from changed_records
    ),

    new_records as (
        select ts.actor,            
            ts.quality_class,
            ts.is_active,
            ts.year as start_date,
            ts.year as end_date
        from this_year_data ts 
        left join last_year_scd ls on ts.actor = ls.actor
        where ls.actor is null
    )

select *, 2021 as year
from (
    select * from historical_scd
    union all
    select * from unchanged_records
    union all
    select * from unnested_changed_records
    union all
    select * from new_records
    ) unioned
;
