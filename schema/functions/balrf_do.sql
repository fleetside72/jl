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
        (SELECT lower(dur) FROM evt.fspr WHERE id = '2018.08'::ltree)
        ,(SELECT lower(dur) FROM evt.fspr WHERE id = '2019.02'::ltree)
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
            ,x.prop->>'func' func
        FROM
            evt.bal b
            INNER JOIN evt.acct x ON
                x.acct = b.acct
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
        WITH RECURSIVE rf (acct, func, re, flag, id, dur, obal, debits, credits, cbal) AS
        (
            SELECT
                a.acct
                ,a.func
                ,a.re
                ,'' flag
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
                CASE COALESCE(dc.flag,'') 
                    WHEN 'clear' THEN rf.acct
                    WHEN 'offset' THEN rf.re
                    ELSE rf.acct 
                END acct
                ,rf.func
                ,rf.re
                ,COALESCE(dc.flag,'') flag
                ,f.id
                ,f.dur
                ,CASE COALESCE(dc.flag,'') 
                    WHEN 'clear' THEN 0
                    WHEN 'offset' THEN rf.cbal
                    ELSE rf.cbal 
                END::numeric(12,2) obal
                ,CASE COALESCE(dc.flag,'') 
                    WHEN 'clear' THEN 0
                    WHEN 'offset' THEN 0
                    ELSE rf.debits
                END::numeric(12,2) debits
                ,CASE COALESCE(dc.flag,'') 
                    WHEN 'clear' THEN 0
                    WHEN 'offset' THEN 0
                    ELSE rf.credits
                END::numeric(12,2) credits
                ,CASE COALESCE(dc.flag,'') 
                    WHEN 'clear' THEN 0
                    WHEN 'offset' THEN rf.cbal
                    ELSE rf.cbal + COALESCE(b.debits,0) + COALESCE(b.credits,0)
                END::numeric(12,2) cbal
                --,(rf.cbal + COALESCE(b.debits,0) + COALESCE(b.credits,0))::NUMERIC(12,2)
            FROM
                rf
                INNER JOIN evt.fspr f ON
                    lower(f.dur) = upper(rf.dur)
                LEFT OUTER JOIN (SELECT * FROM (VALUES ('clear'), ('offset')) X (flag)) dc ON
                    rf.func = 'netinc'
                    AND subpath(rf.id,0,1) <> subpath(f.id,0,1)
                --this join needs to include any currently booked retained earnings
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = CASE COALESCE(dc.flag,'') 
                                WHEN 'clear' THEN rf.acct
                                WHEN 'offset' THEN rf.re
                                ELSE rf.acct 
                             END
                    AND b.fspr = f.id
            WHERE
                lower(f.dur) <= _maxd
        ) 
        select acct, id, sum(obal) obal, sum(debits) debits, sum(credits) credits, sum(cbal) cbal FROM rf GROUP BY acct, id
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