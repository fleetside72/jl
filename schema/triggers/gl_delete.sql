---------------------------handle deleted gl lines----------------------------------------

CREATE OR REPLACE FUNCTION evt.gl_delete() RETURNS trigger
LANGUAGE plpgsql
AS 
$func$
DECLARE
    _mind timestamptz;
    _maxd timestamptz;
BEGIN    
    SELECT 
        (SELECT min(lower(f.dur)) FROM ins INNER JOIN evt.fspr f ON f.id = ins.fspr)
        ,GREATEST(
            (SELECT max(lower(f.dur)) FROM ins INNER JOIN evt.fspr f ON f.id = ins.fspr),
            COALESCE(
                (SELECT max(lower(dur)) FROM evt.fspr WHERE prop @> '{"gltouch":"yes"}'),
                (SELECT max(lower(f.dur)) FROM ins INNER JOIN evt.fspr f ON f.id = ins.fspr)
            )
        )
    INTO
        _mind
        ,_maxd;

    WITH
    agg AS (
        SELECT
            acct
            ,fspr
            ,dur
            --negate initial debits credits
            ,coalesce(-sum(amount) FILTER (WHERE amount > 0),0) debits
            ,coalesce(-sum(amount) FILTER (WHERE amount < 0),0) credits
        FROM
            ins
            INNER JOIN evt.fspr f ON
                f.id = ins.fspr
        GROUP BY
            acct
            ,fspr
            ,dur
    )
    --get every account involved in target range
    ,arng AS (
        SELECT DISTINCT
            acct
        FROM
            agg b
    )
    ,seq AS (
        WITH RECURSIVE rf (acct, id, dur, obal, debits, credits, cbal) AS
        (
            SELECT
                arng.acct
                ,f.id
                ,f.dur
                ,COALESCE(b.obal::numeric(12,2),0)
                ,COALESCE(b.debits::numeric(12,2),0) + COALESCE(agg.debits,0)
                ,COALESCE(b.credits::numeric(12,2),0) + COALESCE(agg.credits,0)
                ,COALESCE(b.cbal::numeric(12,2),0) + COALESCE(agg.debits,0) + COALESCE(agg.credits,0)
            FROM
                arng
                INNER JOIN evt.fspr f ON
                    upper(f.dur) = _mind
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = arng.acct
                    AND b.fspr = f.id
                LEFT OUTER JOIN agg ON
                    agg.acct = arng.acct
                    AND agg.fspr = f.id
            
            UNION ALL

            SELECT
                rf.acct
                ,f.id
                ,f.dur
                ,COALESCE(rf.cbal,0)::numeric(12,2) 
                ,COALESCE(b.debits,0)::numeric(12,2) + COALESCE(agg.debits,0)
                ,COALESCE(b.credits,0)::numeric(12,2) + COALESCe(agg.credits,0)
                ,(COALESCE(rf.cbal,0) + COALESCE(b.debits,0) + COALESCE(b.credits,0))::numeric(12,2) + COALESCE(agg.debits,0) + COALESCE(agg.credits,0)
            FROM
                rf
                INNER JOIN evt.fspr f ON
                    lower(f.dur) = upper(rf.dur)
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = rf.acct
                    AND b.fspr = f.id
                LEFT OUTER JOIN agg ON
                    agg.acct = rf.acct
                    AND agg.fspr = f.id
            WHERE
                lower(f.dur) <= _maxd
        )
        SELECT * FROM rf WHERE lower(dur) >= _mind
    )
    ,bali AS (
        INSERT INTO
            evt.bal (acct, fspr, obal, debits, credits, cbal)
        SELECT
            acct
            ,id
            ,obal
            ,debits
            ,credits
            ,cbal
        FROM
            seq
        ON CONFLICT ON CONSTRAINT bal_pk DO UPDATE SET
            obal = EXCLUDED.obal
            ,debits = EXCLUDED.debits
            ,credits = EXCLUDED.credits
            ,cbal = EXCLUDED.cbal
            ,prop = evt.bal.prop || EXCLUDED.prop
        RETURNING * 
    )
    ,n as (
        SELECT DISTINCT
            fspr
        FROM
            bali
    )
    UPDATE
        evt.fspr f
    SET
        prop = COALESCE(f.prop,'{}'::JSONB) || '{"gltouch":"yes"}'::jsonb
    FROM
        n
    WHERE
        f.id = n.fspr;

    PERFORM evt.balrf();

    RETURN NULL;
END;
$func$;

COMMENT ON FUNCTION evt.gl_delete IS 'reduce evt.bal for deleted ledger rows';

CREATE TRIGGER gl_delete 
    AFTER DELETE ON evt.gl
    REFERENCING OLD TABLE AS ins
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.gl_delete();