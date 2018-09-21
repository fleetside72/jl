---------------------------handle new gl lines----------------------------------------

CREATE OR REPLACE FUNCTION evt.bal_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
        WITH
        --incoming new accounts and any other periods used for the same accounts
        rng AS (
            --for each item determine if a gap exists between new an previous period (if any)
            SELECT
                ins.acct
                ,ins.fspr
                ,lower(f.dur) dur
                ,max(lower(bp.dur)) maxp
                ,min(lower(bp.dur)) minp
            FROM
                evt.bal ins
                INNER JOIN evt.fspr f ON
                    f.id = ins.fspr
                LEFT OUTER JOIN evt.bal b ON
                    b.acct = ins.acct
                LEFT OUTER JOIN evt.fspr bp ON
                    bp.id = b.fspr
            WHERE ins.fspr = '2018.11'
            GROUP BY
                ins.acct
                ,ins.fspr
                ,f.dur
        )
        select
        RETURN NULL;
    END;
    $func$;

CREATE TRIGGER bal_insert 
    AFTER INSERT ON evt.bal
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.bal_insert();