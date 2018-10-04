CREATE FUNCTION evt.balrf() RETURNS void
LANGUAGE plpgsql AS
$func$
DECLARE
    _mind timestamptz;
    _maxd timestamptz;
    _minp ltree;
    _maxp ltree;
BEGIN
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --get last accounts touched and last rollforward if available
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    SELECT
        --get last rollforward, if none, use earliest period touched
        COALESCE(
            MAX(lower(dur)) FILTER (WHERE prop @> '{"rf":"global"}'::jsonb) 
            ,MIN(lower(dur)) FILTER (WHERE prop @> '{"gltouch":"yes"}'::jsonb) 
        ) mind
        --max period touched
        ,MAX(lower(dur)) FILTER (WHERE prop @> '{"gltouch":"yes"}'::jsonb) maxd
    INTO
        _mind
        ,_maxd
    FROM
        evt.fspr;

    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --test if a roll forward is required
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    IF _maxd <= _mind THEN
        RETURN;
    END IF;
            
            
    WITH
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --list each period in min and max
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    prng AS (
        SELECT 
            id
            ,dur
            ,prop
        FROM
            evt.fspr f
        WHERE
            lower(f.dur) >= _mind
            AND lower(f.dur) <= _maxd
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --get every account involved in target range
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,arng AS (
        SELECT DISTINCT
            acct
        FROM
            evt.bal b
            INNER JOIN prng ON
                prng.id = b.fspr
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --cascade the balances
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,bld AS (
        WITH RECURSIVE rf (acct, id, dur, obal, debits, credits, cbal) AS
        (
            SELECT
                a.acct
                ,f.id
                ,f.dur
                ,COALESCE(b.obal,0)::numeric(12,2)
                ,COALESCE(b.debits,0)::numeric(12,2)
                ,COALESCE(b.credits,0)::numeric(12,2)
                ,COALESCE(b.cbal,0)::numeric(12,2)
            FROM
                arng a
                INNER JOIN evt.fspr f ON
                    lower(f.dur) = _mind
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = a.acct
                    AND b.fspr = f.id
            
            UNION ALL

            SELECT
                rf.acct
                ,f.id
                ,f.dur
                ,rf.cbal
                ,COALESCE(b.debits,0)::numeric(12,2)
                ,COALESCE(b.credits,0)::numeric(12,2)
                ,(rf.cbal + COALESCE(b.debits,0) + COALESCE(b.credits,0))::NUMERIC(12,2)
            FROM
                rf
                INNER JOIN evt.fspr f ON
                    lower(f.dur) = upper(rf.dur)
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = rf.acct
                    AND b.fspr = f.id
            WHERE
                lower(f.dur) <= _maxd
        ) 
        select * from rf
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --upsert the cascaded balances
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,ins AS (
        INSERT INTO
            evt.bal
        SELECT
            acct
            ,id
            ,obal
            ,debits
            ,credits
            ,cbal
        FROM 
            bld
        ON CONFLICT ON CONSTRAINT bal_pk DO UPDATE SET
            obal = EXCLUDED.obal
            ,debits = EXCLUDED.debits
            ,credits = EXCLUDED.credits
            ,cbal = EXCLUDED.cbal
        RETURNING *
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --determine all fiscal periods involved
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,touched AS (
        SELECT DISTINCT
            fspr
        FROM
            ins
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --update evt.fsor to reflect roll status
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    UPDATE
        evt.fspr f
    SET
        prop = COALESCE(f.prop,'{}'::jsonb) || '{"rf":"global"}'::jsonb
    FROM
        touched t
    WHERE
        t.fspr = f.id;

    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --get periods to test if the year is changing
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    /*
    SELECT id INTO _minp FROM evt.fspr WHERE lower(dur) = _mind;
    SELECT id INTO _maxp FROM evt.fspr WHERE lower(dur) = _maxd;

    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --if the top level item is greater for max than min then the year is changing
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    IF subpath(_maxp,0,1) > subpath(_minp,0,1) THEN
        --get entry to close year
        --SELECT * FROM evt.closeyear(_year)
    END IF;
    */

END;
$func$;

COMMENT ON FUNCTION evt.balrf() IS 'close any gaps and ensure all accounts roll forward';