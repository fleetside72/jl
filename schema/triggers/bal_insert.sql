---------------------------handle new gl lines----------------------------------------

CREATE OR REPLACE FUNCTION evt.bal_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
        WITH
        seq AS (
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
                            (
                                SELECT
                                    *
                                FROM
                                    evt.bal
                                WHERE
                                    fspr = '2018.11'
                            ) ins
                            INNER JOIN evt.fspr f ON
                                f.id = ins.fspr
                            LEFT OUTER JOIN evt.bal b ON
                                b.acct = ins.acct
                            LEFT OUTER JOIN evt.fspr bp ON
                                bp.id = b.fspr
                        WHERE
                            b.fspr <> '2018.11'
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
        select * from seq
        ,xseq AS (
        SELECT
            rng.*
            ,fspr.id
            ,row_number() OVER (partition by rng.acct, rng.fspr ORDER BY lower(fspr.dur) ASC) rn
            ,coalesce(b.obal,0) obal
            ,coalesce(b.debits,0) debits
            ,coalesce(b.credits,0) credits
            ,coalesce(b.cbal,0) cbal
        FROM
            rng
            INNER JOIN evt.fspr ON
                lower(fspr.dur) >= minrange
                AND lower(fspr.dur) <= maxrange
            LEFT OUTER JOIN evt.bal b ON
                b.acct = rng.acct
                AND b.fspr = fspr.id
        ORDER BY
            rng.acct
            ,rng.fspr
            ,rn
        )
        select * from seq
        RETURN NULL;
    END;
    $func$;

CREATE TRIGGER bal_insert 
    AFTER INSERT ON evt.bal
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.bal_insert();