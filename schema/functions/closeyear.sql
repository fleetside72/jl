CREATE OR REPLACE FUNCTION evt.closeyear(_year ltree) RETURNS TABLE (fspr ltree, acct ltree, cbal numeric(12,2)) LANGUAGE plpgsql AS
DO
$do$
DECLARE
    _year ltree;
    _lastt timestamptz;
    _lastl ltree;
BEGIN
    SELECT '2018'::ltree INTO _year;
    RAISE NOTICE 'target year: %',_year;

    SELECT max(lower(dur)) INTO _lastt FROM evt.fspr WHERE id <@ _year;
    RAISE NOTICE 'last timestamp: %',_lastt;

    SELECT id INTO _lastl FROM evt.fspr WHERE lower(dur) = _lastt;
    RAISE NOTICE 'last fsical period: %',_lastl;

    --build retained earnings accounts everytime this function is called (bad idea?)
    --get list of tb's
    WITH
    tb AS (
    SELECT DISTINCT
        subpath(acct,0,1) tb
    FROM
        evt.acct
    )
    --associated or otherwise build a retained earnings account
    ,re AS (
    SELECT
        tb
        ,COALESCE(a.acct,tb::ltree||'re'::ltree) acct
        ,COALESCE(a.prop,'{"retained_earnings":"set"}') prop
    FROM
        tb
        LEFT OUTER JOIN evt.acct a ON
            subpath(a.acct,0,1) = tb.tb
            AND a.prop @> '{"retained_earnings":"set"}'::jsonb
    )
    --re-insert all accounts and if they already exist do nothing
    INSERT INTO
        evt.acct
    SELECT
        re.acct
        ,re.prop
    FROM
        re
    ON CONFLICT DO NOTHING;
    
    DROP TABLE IF EXISTS test;
    CREATE TEMP TABLE test AS (
    WITH
    dc AS (
        SELECT * FROM (VALUES (true), (false)) X (FLAG)
    )
    SELECT
        b.fspr
        ,dc.flag
        ,CASE WHEN dc.flag THEN b.acct ELSE re.acct END acct
        ,sum(CASE WHEN dc.flag THEN -b.cbal ELSE b.cbal END) cbal 
    FROM
        evt.bal b
        CROSS JOIN dc
        --join ot master
        INNER JOIN evt.acct a ON
            a.acct = b.acct
        --get retained earnings
        LEFT OUTER JOIN evt.acct re ON
            b.acct <@ subpath(re.acct,0,1)
            AND re.prop @> '{"retained_earnings":"set"}'::jsonb
    WHERE
        fspr = _lastl
        AND a.prop @> '{"func":"netinc"}'::jsonb
        --AND temp accounts only
    GROUP BY
        b.fspr
        ,dc.flag
        ,CASE WHEN dc.flag THEN b.acct ELSE re.acct END
    ) with data;
        /*filter for tb?*/

    --carry new balances forward
    /*
    UPDATE evt.bal b
    FROM (SELECT * FROM evt.bal WHERE fspr = _lastl) l
    SET b.obal = l.cbal
    WHERE b.acct = l.acct;
    */
END;
$do$;
SELECT * FROM TEST
