---------------------------handle new gl lines----------------------------------------

CREATE OR REPLACE FUNCTION evt.gl_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
        WITH
        agg AS (
            SELECT
                acct
                ,fspr
                ,coalesce(sum(amount) FILTER (WHERE amount > 0),0) debits
                ,coalesce(sum(amount) FILTER (WHERE amount < 0),0) credits
            FROM
                ins
            GROUP BY
                acct
                ,fspr
        )
        ,ins AS (
            INSERT INTO
                evt.bal
            SELECT
                acct
                ,fspr
                ,0 obal
                ,debits
                ,credits
                ,debits + credits
            FROM
                agg
            ON CONFLICT ON CONSTRAINT bal_pk DO UPDATE SET
                debits = evt.bal.debits + EXCLUDED.debits
                ,credits = evt.bal.credits + EXCLUDED.credits
                ,cbal = evt.bal.cbal + EXCLUDED.debits + EXCLUDED.credits;
        )
        ,seq AS (
            WITH RECURSIVE rf (acct, fspr, minrange, maxrange, dur, id, obal, debits, credits, cbal) AS
            (
                SELECT
                    rng.acct
                    ,rng.fspr
                    ,rng.minrange
                    ,rng.maxrange
                    ,f.dur
                    ,f.id
                    ,b.obal::numeric(12,2)
                    ,b.debits::numeric(12,2)
                    ,b.credits::numeric(12,2)
                    ,b.cbal::numeric(12,2)
                FROM
                    (
                        --for each item determine if a gap exists between new an previous period (if any)
                        SELECT
                            ins.acct
                            ,ins.fspr
                            ,lower(f.dur) dur
                            ,CASE WHEN lower(f.dur) > max(lower(bp.dur)) THEN max(lower(bp.dur)) ELSE lower(f.dur) END minrange
                            ,CASE WHEN lower(f.dur) < max(lower(bp.dur)) THEN max(lower(bp.dur)) ELSE lower(f.dur) END maxrange
                        FROM
                            ins
                            INNER JOIN evt.fspr f ON
                                f.id = ins.fspr
                            LEFT OUTER JOIN evt.bal b ON
                                b.acct = ins.acct
                            LEFT OUTER JOIN evt.fspr bp ON
                                bp.id = b.fspr
                        GROUP BY
                            ins.acct
                            ,ins.fspr
                            ,f.dur
                    ) rng
                    INNER JOIN evt.fspr f ON
                        lower(f.dur) = minrange
                    INNER JOIN evt.bal b ON 
                        b.acct = rng.acct
                        AND b.fspr = f.id
                
                UNION ALL

                SELECT
                    rf.acct
                    ,rf.fspr
                    ,rf.minrange
                    ,rf.maxrange
                    ,f.dur
                    ,f.id
                    ,COALESCE(rf.cbal,0)::numeric(12,2)
                    ,COALESCE(b.debits,0)::numeric(12,2)
                    ,COALESCE(b.credits,0)::numeric(12,2)
                    ,(COALESCE(rf.cbal,0) + COALESCE(b.debits,0) + COALESCE(b.credits,0))::numeric(12,2)
                FROM
                    rf
                    INNER JOIN evt.fspr f ON
                        lower(f.dur) = upper(rf.dur)
                    LEFT OUTER JOIN evt.bal b ON
                        b.acct = rf.acct
                        AND b.fspr = f.id
                WHERE
                    lower(f.dur) <= rf.maxrange
            )
            SELECT * FROM rf
        )
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
            ,cbal = EXCLUDED.cbal;
        RETURN NULL;
    END;
    $func$;

CREATE TRIGGER gl_insert 
    AFTER INSERT ON evt.gl
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.gl_insert();