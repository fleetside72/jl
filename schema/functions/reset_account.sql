
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

DO
$f$
DECLARE
    _json jsonb;
    _key text;
    _oldval text;
    _newval text;
    _old text;
    _new text;
BEGIN
    _json := '{"gl": {"jpath": [["{item,0}", "{header}"], ["{item,0}", "{header}"]], "lines": [{"amount": 4.00, "account": "h.cars"}, {"amount": -4.00, "account": "h.dcard"}]}, "item": [{"item": "light bulb", "amount": 4.00, "reason": "repair", "account": "h.cars"}], "header": {"date": "2018-09-01", "entity": "home", "module": "MHI", "offset": "h.dcard", "transaction": "purchase"}}'::jsonb;
    _key := 'account';
    _oldval := 'h.cars';
    _newval := 'h.x';
    _old := '"'||_key||'": "'||_oldval||'"';
    _new := '"'||_key||'": "'||_newval||'"';
    RAISE NOTICE '%',_old;
    RAISE NOTICE '%',_new;
    RAISE NOTICE '%',regexp_replace(_json::text,'"'||_key||'": "'||_oldval||'"','"'||_key||'": "'||_newval||'"','g');
END;
$f$
language plpgsql

DROP FUNCTION IF EXISTS evt.kv_replace(_jsonb jsonb, _key text, _oldval text, _newval text);
CREATE OR REPLACE FUNCTION evt.kv_replace(_jsonb jsonb, _key text, _oldval text, _newval text) RETURNS jsonb
AS
$f$
DECLARE
    _result jsonb;
BEGIN

    SELECT regexp_replace(_jsonb::text,'"'||_key||'": "'||_oldval||'"','"'||_key||'": "'||_newval||'"','g') INTO _result;

    RETURN _result;
END;
$f$
language plpgsql