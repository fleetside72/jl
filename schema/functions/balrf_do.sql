DO
$func$
DECLARE
    _mind timestamptz;
    _maxd timestamptz;
    _minp ltree;
    _maxp ltree;
BEGIN
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --get last periods touched and last rollforward if available
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    SELECT
        MIN(lower(dur)) FILTER (WHERE prop @> '{"gltouch":"yes"}'::jsonb) 
        ,MAX(lower(dur)) FILTER (WHERE prop @> '{"gltouch":"yes"}'::jsonb) maxd
    INTO
        _mind
        ,_maxd
    FROM
        evt.fspr;

    RAISE NOTICE 'earliest stamp%',_mind;
    RAISE NOTICE 'latest stamp%',_maxd;

    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --test if a roll forward is required
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    IF _maxd <= _mind THEN
        RETURN;
    END IF;
            
    CREATE TEMP TABLE r AS (
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
            b.acct
            ,a.acct re
        FROM
            evt.bal b
            INNER JOIN prng ON
                prng.id = b.fspr
            LEFT OUTER JOIN evt.acct a ON
                subpath(a.acct,0,1) = subpath(b.acct,0,1)
                AND a.prop @> '{"retained_earnings":"set"}'::jsonb
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --cascade the balances
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,bld AS (
        WITH RECURSIVE rf (acct, re, id, dur, obal, debits, credits, cbal) AS
        (
            SELECT
                a.acct
                ,a.re
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
                ,rf.re
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
                LEFT OUTER JOIN (SELECT * FROM (VALUES (true), (false)) X (flag)) dc ON
                    subpath(rf.id,0,1) <> subpath(f.id,0,1)
            WHERE
                lower(f.dur) <= _maxd
        ) 
        select * from rf
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --upsert the cascaded balances
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    SELECT
        acct
        ,id
        ,obal
        ,debits
        ,credits
        ,cbal
    FROM 
        bld
    ) WITH DATA;
  

END;
$func$;
SELECT * FROM r order by acct, id