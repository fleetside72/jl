---------------------------handle balance updates----------------------------------------

CREATE OR REPLACE FUNCTION evt.bal_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
        WITH
        list AS (
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
                    ,b.obal::numeric(12,2)
                    ,b.debits::numeric(12,2)
                    ,b.credits::numeric(12,2)
                    ,b.cbal::numeric(12,2)
                FROM
                    list
                    INNER JOIN evt.fspr f ON
                        lower(f.dur) = list.minp
                    LEFT OUTER JOIN evt.bal b ON
                        b.acct = list.acct
                        AND b.fspr = f.id
                
                UNION ALL

                SELECT
                    rf.acct
                    ,rf.minp
                    ,rf.maxp
                    ,f.id
                    ,f.dur
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
                    lower(f.dur) <= rf.maxp
            )
            select * from rf
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

CREATE TRIGGER bal_insert 
    AFTER INSERT ON evt.bal
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.bal_insert();