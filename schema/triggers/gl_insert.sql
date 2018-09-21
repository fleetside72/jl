---------------------------handle new gl lines----------------------------------------

CREATE OR REPLACE FUNCTION evt.gl_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    --upsert gl balance
    --if a new period is created roll any gaps
    WITH
    agg AS (
        SELECT
            acct
            ,fspr
            ,sum(amount) FILTER (WHERE amount > 0) debits
            ,sum(amount) FILTER (WHERE amount < 0) credits
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
        ,0
        ,debits
        ,credits
        ,debits + credits
    FROM
        agg
    ON CONFLICT ON CONSTRAINT PRIMARY KEY DO UPDATE SET
        debits = debits + EXCLUDED.debits
        ,credits = credits + EXCLUDED.credits
        ,cbal = cbal + debits + credits