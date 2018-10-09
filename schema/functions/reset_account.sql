
select
        g.id
        ,jp.j::text[]
        ,jp.rn
        ,jp.j::text[]
        ,jsonb_pretty(jsonb_set(bpr,(jp.j::text[]||ARRAY['account']),'"h.cars"'))
        ,jsonb_pretty(bpr)
from
        evt.gl g
        inner join evt.bpr b on
                b.id = g.bprid
        join lateral jsonb_array_elements_text(b.bpr->'gl'->'jpath'->glline) WITH ORDINALITY jp(j, rn) on
                b.bpr#>(jp.j::text[])->>'account' = g.acct::text
where
        g.acct = 'h.maint.cars';


select regexp_replace(bpr::text,'"account": "h.maint.cars"','"account": "h.cars"','g') from evt.bpr where bpr::text ~ '"account": "h.maint.cars"'