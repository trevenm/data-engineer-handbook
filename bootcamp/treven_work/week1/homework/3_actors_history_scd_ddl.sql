create table actors_history_scd (
    actor text,
    quality_class quality_class,
    is_active boolean,    
    start_date int,
    end_date int,
    year int,
    primary key(actor, start_date)
)
;