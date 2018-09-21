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
        RETURN NULL;
    END;
    $func$;

CREATE TRIGGER gl_insert 
    AFTER INSERT ON evt.gl
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT
    EXECUTE PROCEDURE evt.gl_insert();