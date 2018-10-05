---------------------------handle new gl lines----------------------------------------

CREATE OR REPLACE FUNCTION evt.gl_insert() RETURNS trigger
LANGUAGE plpgsql
AS 
$func$
DECLARE
    _mind timestamptz;
    _maxd timestamptz;
BEGIN    
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --find min and max applicable periods to roll
    --min: earliest period involved in current gl posting
    --max: latest period involved in any posting, or if none, the current posting
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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

    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --aggregate all inserted gl transactions
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    WITH
    agg AS (
        SELECT
            acct
            ,fspr
            ,dur
            ,coalesce(sum(amount) FILTER (WHERE amount > 0),0) debits
            ,coalesce(sum(amount) FILTER (WHERE amount < 0),0) credits
        FROM
            ins
            INNER JOIN evt.fspr f ON
                f.id = ins.fspr
        GROUP BY
            acct
            ,fspr
            ,dur
    )
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --get every account involved in target range
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,arng AS (
        SELECT DISTINCT
            acct
        FROM
            agg b
    )
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --roll the balances forward
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,bld AS (
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
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --insert the rolled balances
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,ins AS (
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
            bld
        ON CONFLICT ON CONSTRAINT bal_pk DO UPDATE SET
            obal = EXCLUDED.obal
            ,debits = EXCLUDED.debits
            ,credits = EXCLUDED.credits
            ,cbal = EXCLUDED.cbal
            ,prop = evt.bal.prop || EXCLUDED.prop
        RETURNING * 
    )
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --determine all fiscal periods invovled in the insert
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,touched as (
        SELECT DISTINCT
            fspr
        FROM
            ins
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --update evt.fspr to reflect touched by gl
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    UPDATE
        evt.fspr f
    SET
        prop = COALESCE(f.prop,'{}'::JSONB) || '{"gltouch":"yes"}'::jsonb
    FROM
        touched t
    WHERE
        f.id = t.fspr;

    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --this is to catch up all the other accounts if actually necessary
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    PERFORM evt.balrf();

    RETURN NULL;
END;
$func$;

COMMENT ON FUNCTION evt.gl_insert IS 'update evt.bal with new ledger rows';

CREATE TRIGGER gl_insert 
    AFTER INSERT ON evt.gl
    REFERENCING NEW TABLE AS ins
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.gl_insert();