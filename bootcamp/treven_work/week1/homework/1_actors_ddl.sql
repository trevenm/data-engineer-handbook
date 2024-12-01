-- drop type films cascade;
create type films as (
    film text,
    year int,
    votes int,
    rating real,
    filmid text
)
;

create type quality_class
    as enum(
        'star',
        'good',
        'average',
        'bad'
    )
;

-- drop table actors;
create table actors (
    actor text,
    actorid text,
    films films[],
    quality_class quality_class,
    year int,
    years_since_last_active int,
    is_active boolean,
    primary key (actorid, year)
)
;

