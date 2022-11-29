with

deduped as (
    select *
    from {{ source('xg', 'model_xg*') }} as conferences

    qualify row_number() over(
        partition by
            id_play_id
            , id_player_id
    ) = 1
)

select * from deduped
