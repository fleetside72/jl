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
            SELECT
                acct
                ,fspr
                ,debits
                ,credits
            FROM
                agg
        )
        ,list AS (
            SELECT 
                acct
                ,min(lower(dur)) minp
                ,max(lower(dur)) maxp
            FROM
                ins b
                INNER JOIN evt.fspr f ON
                    f.id = b.fspr
            GROUP BY
                acct
        )
        ,seq AS (
            WITH RECURSIVE rf (acct, minp, maxp, id, dur, obal, debits, credits, cbal) AS
            (
                SELECT
                    list.acct
                    ,list.minp
                    ,list.maxp
                    ,f.id
                    ,f.dur
                    ,COALESCE(b.obal::numeric(12,2),0)
                    ,COALESCE(b.debits::numeric(12,2),0) + COALESCE(ins.debits,0)
                    ,COALESCE(b.credits::numeric(12,2),0) + COALESCE(ins.credits,0)
                    ,COALESCE(b.cbal::numeric(12,2),0) + COALESCE(ins.debits,0) + COALESCE(ins.credits,0)
                FROM
                    list
                    INNER JOIN evt.fspr f ON
                        upper(f.dur) = list.minp
                    LEFT OUTER JOIN evt.bal b ON
                        b.acct = list.acct
                        AND b.fspr = f.id
                    LEFT OUTER JOIN ins ON
                        ins.acct = list.acct
                        AND ins.fspr = f.id
                
                UNION ALL

                SELECT
                    rf.acct
                    ,rf.minp
                    ,rf.maxp
                    ,f.id
                    ,f.dur
                    ,COALESCE(rf.cbal,0)::numeric(12,2) 
                    ,COALESCE(b.debits,0)::numeric(12,2) + COALESCE(ins.debits,0)
                    ,COALESCE(b.credits,0)::numeric(12,2) + COALESCe(ins.credits,0)
                    ,(COALESCE(rf.cbal,0) + COALESCE(b.debits,0) + COALESCE(b.credits,0))::numeric(12,2) + COALESCE(ins.debits,0) + COALESCE(ins.credits,0)
                FROM
                    rf
                    INNER JOIN evt.fspr f ON
                        lower(f.dur) = upper(rf.dur)
                    LEFT OUTER JOIN evt.bal b ON
                        b.acct = rf.acct
                        AND b.fspr = f.id
                    LEFT OUTER JOIN ins ON
                        ins.acct = rf.acct
                        AND ins.fspr = f.id
                WHERE
                    lower(f.dur) <= rf.maxp
            )
            SELECT * FROM rf WHERE lower(dur) >= minp
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