WITH
--startign month
m as (
    SELECT
        *
    FROM
        (
            VALUES
            (1,1,1)
            ,(2,2,1)
            ,(3,3,1)
            ,(4,4,2)
            ,(5,5,2)
            ,(6,6,2)
            ,(7,7,3)
            ,(8,8,3)
            ,(9,9,3)
            ,(10,10,4)
            ,(11,11,4)
            ,(12,12,4)
        ) X (cm,fm,fq)
)
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
    generate_series('2018-01-01 00:00'::timestamptz,'2099-12-01 00:00'::timestamptz,'1 month') gs(d)
    INNER JOIN m ON
        m.cm = extract(month from gs.d)