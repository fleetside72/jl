DO
$do$
DECLARE
    _year ltree;
    _lastt timestamptz;
    _lastl ltree;
BEGIN
    SELECT '2018'::ltree INTO _year;
    RAISE NOTICE '%',_year;

    SELECT max(lower(dur)) INTO _lastt FROM evt.fspr WHERE id <@ _year;
    RAISE NOTICE '%',_lastt;

    SELECT id INTO _lastl FROM evt.fspr WHERE lower(dur) = _lastt;
    RAISE NOTICE '%',_lastl;

    --if no reatined earnings account exists when this function is called add h.re

    SELECT
        b.fspr
        ,re.acct re
        ,sum(b.cbal)
    FROM
        evt.bal b
        INNER JOIN evt.acct a ON
            a.acct = b.acct
        LEFT OUTER JOIN evt.acct re ON
            b.acct <@ subpath(re.acct,0,1)
            AND re.prop @> '{"retained_earnings":"set"}'::jsonb
    GROUP BY
        b.fspr
        ,re.acct
    WHERE
        fspr = _lastl
        AND a.prop @> '{"state":"temporary"}' --if states change, a history should be reatined, then should history be used for this roll, or current definition?
        /*filter for tb?*/

    --carry new balances forward
    UPDATE evt.bal b
    FROM (SELECT * FROM evt.bal WHERE fspr = _lastl) l
    SET b.obal = l.cbal
    WHERE b.acct = l.acct;
END
$do$