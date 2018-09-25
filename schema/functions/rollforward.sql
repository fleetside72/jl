CREATE FUNCTION evt.balrf(_mind tstzrange, _maxd tstzrange) RETURNS void AS
$func$
DECLARE
    _lastdur tstzrange;
    _newdur tstzrange;
BEGIN
    
    --get last global rollforward
    SELECT
        dur
    INTO
        _lastdur
    FROM
        evt.fspr
    WHERE
        prop @> '{"rf":"global"}'::jsonb
    WITH;

    
    WITH
    d AS (
        SELECT DISTINCT fspr FROM ins
    )
    SELECT
        max(f.dur)
    INTO
        _newdur
    FROM
        d
        INNER JOIN evt.fspr f ON
            f.id = d.id;
            
    WITH
    --list each period in min and max
    prng AS (
        SELECT 
            id
            ,dur
            ,prop
        FROM
            evt.fspr f
        WHERE
            f.dur >= _mind
            AND f.dur <= _maxd
    )
    --get every account involved in target range
    ,arng AS (
        SELECT DISTINCT
            acct
        FROM
            evt.bal b
            INNER JOIN prng ON
                prng.id = b.fspr
    )
    ,bld AS (
        WITH RECURSIVE rf (acct, id, dur, propr, obal, debits, credits, cbal) AS
        (
            SELECT
                a.acct
                ,f.id
                ,f.dur
                ,f.prop
                ,COALESCE(b.obal,0)::numeric(12,2)
                ,COALESCE(b.debits,0)::numeric(12,2)
                ,COALESCE(b.credits,0)::numeric(12,2)
                ,COALESCE(b.cbal,0)::numeric(12,2)
            FROM
                arng a
                INNER JOIN evt.fspr f ON
                    lower(f.dur) = (SELECT mind FROM minmax)
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = a.acct
                    AND b.fspr = f.id
            
            UNION ALL

            SELECT
                rf.acct
                ,f.id
                ,f.dur
                ,f.prop
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
                lower(f.dur) <= (SELECT maxd FROM minmax)
        ) 
        select * from rf
    )
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
        ,cbal = EXCLUDED.cbal;
END;
$func$
LANGUAGE plpgsql