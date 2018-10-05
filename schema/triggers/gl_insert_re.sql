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
            --the last period inserted
            (SELECT max(lower(f.dur)) FROM ins INNER JOIN evt.fspr f ON f.id = ins.fspr),
            --or the last period touched anywhere, or if null, the last period inserted to
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
    --get every account touched in the transaction
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,arng AS (
        SELECT DISTINCT
            b.acct
            --if no retained earnings account exists then automitically create it (assuming no other account is called re)
            ,COALESCE(a.acct,subpath(b.acct,0,1)||'re'::ltree) re
            ,a.acct existing_re
            ,x.prop->>'func' func
        FROM
            agg b
            --account master
            INNER JOIN evt.acct x ON
                x.acct = b.acct
            LEFT OUTER JOIN evt.acct a ON
                subpath(a.acct,0,1) = subpath(b.acct,0,1)
                AND a.prop @> '{"retained_earnings":"set"}'::jsonb
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --if the default retained earnings account was null, insert the new one to evt.acct
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,new_re AS (
        INSERT INTO
            evt.acct (acct, prop)
        SELECT DISTINCT
            re, '{"retained_earnings":"set"}'::jsonb
        FROM
            arng
        WHERE
            existing_re IS NULL
        RETURNING *
    )
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --roll the balances forward
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,bld AS (
        WITH RECURSIVE rf (acct, func, re, flag, id, dur, obal, debits, credits, cbal, incr_re) AS
        (
            SELECT
                a.acct
                ,a.func
                ,a.re
                ,null::BOOLEAN
                ,f.id
                ,f.dur
                ,COALESCE(b.obal::numeric(12,2),0)
                ,(COALESCE(b.debits,0) + COALESCE(agg.debits,0))::numeric(12,2)
                ,(COALESCE(b.credits,0) + COALESCE(agg.credits,0))::numeric(12,2)
                ,(COALESCE(b.cbal,0) + COALESCE(agg.debits,0) + COALESCE(agg.credits,0))::numeric(12,2)
                ,CASE func WHEN 'netinc' THEN (COALESCE(agg.debits,0) + COALESCE(agg.credits,0))::numeric(12,2) ELSE 0 END incr_re
            FROM
                arng a
                INNER JOIN evt.fspr f ON
                    upper(f.dur) = _mind
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = a.acct
                    AND b.fspr = f.id
                LEFT OUTER JOIN agg ON
                    agg.acct = a.acct
                    AND agg.fspr = f.id
            
            UNION ALL

            SELECT
                --if the year is changing a duplicate join will happen which will allow moving balances to retained earnings
                --the duplicate join happens only for accounts flagged as temporary and needing closed to retained earnings
                --on the true side, the account retains its presence but takes on a zero balance
                --on the false side, the account is swapped out for retained earngings accounts and take on the balance of the expense account
                --if duplciate does not join itself, then treat as per anchor query above and continue aggregating balances for the target range
                CASE dc.flag WHEN true THEN rf.acct     WHEN false THEN rf.re                                       ELSE rf.acct                                                                                                               END acct
                ,rf.func
                ,rf.re
                ,dc.flag
                ,f.id
                ,f.dur
                --                                                   this column needs to pickup existing re
                --if h.food is already in retained earnings and then you add in the entire re-rolled balance again it will be doubled up
                --only the increment of the original transaction should be added to retained earnings
                --the incremental retained earnings needs to survive even past the first dump to ret earn just in case a second year-end is encountered
                ,CASE dc.flag WHEN true THEN 0          WHEN false THEN COALESCE(rf.incr_re,0) + COALESCE(b.obal,0) ELSE COALESCE(rf.cbal,0)                                                                                                   END::numeric(12,2) obal
                ,CASE dc.flag WHEN true THEN 0          WHEN false THEN 0                                           ELSE COALESCE(b.debits,0) + COALESCE(agg.debits,0)                                                                         END::numeric(12,2) debits
                ,CASE dc.flag WHEN true THEN 0          WHEN false THEN 0                                           ELSE COALESCE(b.credits,0) + COALESCe(agg.credits,0)                                                                       END::numeric(12,2) credits
                ,CASE dc.flag WHEN true THEN 0          WHEN false THEN COALESCE(rf.incr_re,0) + COALESCE(b.obal,0) ELSE COALESCE(rf.cbal,0) + COALESCE(b.debits,0) + COALESCE(b.credits,0) + COALESCE(agg.debits,0) + COALESCE(agg.credits,0) END::numeric(12,2) cbal
                ,CASE dc.flag WHEN true THEN rf.incr_re WHEN false THEN COALESCE(rf.incr_re,0) + COALESCE(b.obal,0) ELSE COALESCE(rf.incr_re,0) + COALESCE(agg.debits,0) + COALESCE(agg.credits,0)                                             END::numeric(12,2) incr_re
            FROM
                rf
                INNER JOIN evt.fspr f ON
                    lower(f.dur) = upper(rf.dur)
                LEFT OUTER JOIN (SELECT * FROM (VALUES (true), (false)) X (flag)) dc ON
                    rf.func = 'netinc'
                    AND subpath(rf.id,0,1) <> subpath(f.id,0,1)
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = CASE dc.flag
                                WHEN true THEN rf.acct
                                WHEN false THEN rf.re
                                ELSE rf.acct 
                             END
                    AND b.fspr = f.id
                LEFT OUTER JOIN agg ON
                    agg.acct = CASE dc.flag
                                WHEN true THEN rf.acct
                                WHEN false THEN rf.re
                                ELSE rf.acct 
                             END
                    AND agg.fspr = f.id
            WHERE
                lower(f.dur) <= _maxd
        )
        SELECT 
            acct
            ,id
            ,SUM(obal) obal
            ,SUM(debits) debits
            ,SUM(credits) credits
            ,SUM(cbal) cbal 
        FROM 
            rf 
        GROUP BY 
            acct
            ,id
    )
    -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --insert the balances
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
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --determine all fiscal periods involved
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    ,touched as (
        SELECT DISTINCT
            fspr
        FROM
            ins
    )
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    --update evt.fspr to reflect roll status
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