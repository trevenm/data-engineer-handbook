insert into actors
WITH 
    years as (
        select *
        from generate_series(1970, 2021) as year
    ), 

    actor AS (
        select
            actor,
            min(year) as first_film_year
        from actor_films
        group by actor
    ), 

    actors_and_years AS (
        select *
        from actor a
        inner join years y
            on a.first_film_year <= y.year
    ), 

    windowed as (
        select
            aay.actor,
            aay.year,
            array_remove(
                array_agg(
                    case
                        when af.year is not null
                            then row(
                                af.film,
                                af.year,
                                af.votes,
                                af.rating,
                                af.filmid
                            )::films
                    end)
                over (partition by aay.actor order by coalesce(aay.year, af.year)),
                null
            ) as films
        from actors_and_years aay
        left join actor_films af
            on aay.actor = af.actor
            and aay.year = af.year
        order by aay.actor, aay.year
    ),

    static as (
        select
            actor,
            max(actorid) as actorid
        from actor_films
        group by actor
    )

select distinct 
    w.actor,
    s.actorid,
    w.films,
    case
        when (films[cardinality(films)]::films).rating > 8 then 'star'
        when (films[cardinality(films)]::films).rating > 7 then 'good'
        when (films[cardinality(films)]::films).rating > 6 then 'average'
        else 'bad'
    end::quality_class as quality_class,
    w.year,
    w.year - (films[cardinality(films)]::films).year as years_since_last_active,    
    (films[cardinality(films)]::films).year = year as is_active
from windowed w
join static s
    on w.actor = s.actor
;
