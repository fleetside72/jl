WITH
--startign month
startm AS (
    SELECT 1 as m
)
,m AS (
    SELECT
        g.s calendar_month
        ,startm.m starting_month
        ,g.s + CASE WHEN g.s < startm.m THEN startm.m +1 ELSE -startm.m +1 END fisc_month
    FROM
        generate_series(1,12,1) g(s)
        CROSS JOIN startm
)
--select * from m
INSERT INTO
    evt.fspr
SELECT 
    --TO_CHAR(gs.d,'YYYY.MM.DD')::ltree t1
    (
        --year
        to_char(extract(year from gs.d),'FM0000')
        --month
        ||'.'||to_char(m.fisc_month,'FM00')
    )::ltree t2
    ,tstzrange(gs.d,gs.d + '1 month'::interval) r
FROM 
    generate_series('2018-01-01 00:00'::timestamptz,'2099-12-01 00:00'::timestamptz,'1 month') gs(d)
    INNER JOIN m ON
        m.calendar_month = extract(month from gs.d)
ORDER BY 
    gs.d ASC
/*
INSERT INTO
    evt.fspr
SELECT 
    --TO_CHAR(gs.d,'YYYY.MM.DD')::ltree t1
    (
        --year
        to_char(extract(year from gs.d),'FM0000')
        --quarter
        ||'.'||to_char(m.fq,'FM00')
        --month
        ||'.'||to_char(m.fm,'FM00')
        --day
        ||'.'||to_char(extract(day from gs.d),'FM00')
    )::ltree t2
    ,tstzrange(gs.d,gs.d + '1 month'::interval) r
FROM 
    generate_series('2018-01-01 00:00'::timestamptz,'2099-12-01 00:00'::timestamptz,'1 day') gs(d)
    INNER JOIN m ON
        m.cm = extract(month from gs.d)
*/