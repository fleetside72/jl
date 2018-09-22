BEGIN TRANSACTION;
--\conninfo
--------------------------build schema----------------------------------------------

DROP SCHEMA IF EXISTS evt cascade;
CREATE SCHEMA evt;
CREATE EXTENSION IF NOT EXISTS ltree;
COMMENT ON SCHEMA evt IS 'event log';

--------------------------event log table-------------------------------------------

CREATE TABLE evt.bpr (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,bpr JSONB
    ,bprh JSONB
    ,stmp timestamptz
);
COMMENT ON COLUMN evt.bpr.bpr IS 'basic pecuniary record';
COMMENT ON COLUMN evt.bpr.bprh IS 'basic pecuniary record history';
COMMENT ON COLUMN evt.bpr.stmp IS 'insert time';

--------------------------account master---------------------------------------------

--the account master should be dynamically created
CREATE TABLE evt.acct (
    acct ltree PRIMARY KEY
    ,prop jsonb
);
COMMENT ON COLUMN evt.acct.acct IS 'account';
COMMENT ON COLUMN evt.acct.prop IS 'properties';

------------------------fiscal periods------------------------
CREATE TABLE evt.fspr (
    id ltree PRIMARY KEY
    ,dur tstzrange
    ,prop jsonb
);

COMMENT ON COLUMN evt.fspr.id IS 'fiscal period';
COMMENT ON COLUMN evt.fspr.dur IS 'duration of period as timestamp range';
COMMENT ON COLUMN evt.fspr.prop IS 'period properties';
CREATE INDEX id_gist ON evt.fspr USING GIST (id);



--------------------------relational ledger------------------------------------------

CREATE TABLE evt.gl (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,bprid INT REFERENCES evt.bpr (id)
    ,acct ltree REFERENCES evt.acct (acct)
    ,pstmp timestamptz DEFAULT CURRENT_TIMESTAMP
    --populates by trigger join to evt.fspr
    ,tstmp timestamptz
    ,fspr ltree NOT NULL REFERENCES evt.fspr (id)
    ,amount numeric (12,2)
    ,glline INT
    ,bprkeys JSONB
);
COMMENT ON COLUMN evt.gl.id IS 'gl id';
COMMENT ON COLUMN evt.gl.bprid IS 'id of initial basic pecuniary record';
COMMENT ON COLUMN evt.gl.acct IS 'account code';
COMMENT ON COLUMN evt.gl.pstmp IS 'post time stamp';
COMMENT ON COLUMN evt.gl.tstmp IS 'transaction time stamp';
COMMENT ON COLUMN evt.gl.fspr IS 'fiscal period';
COMMENT ON COLUMN evt.gl.amount IS 'amount';
COMMENT ON COLUMN evt.gl.glline IS 'gl line number';
COMMENT ON COLUMN evt.gl.bprkeys IS 'extract from initial basic pecuniary record';

--------------------------balances----------------------------------------------------

CREATE TABLE evt.bal (
    acct ltree REFERENCES evt.acct(acct)
    ,fspr ltree REFERENCES evt.fspr(id)
    ,obal numeric(12,2)
    ,debits numeric(12,2)
    ,credits numeric(12,2)
    ,cbal numeric(12,2)
    ,prop jsonb
);
ALTER TABLE evt.bal ADD CONSTRAINT bal_pk PRIMARY KEY(acct,fspr);
COMMENT ON COLUMN evt.bal.acct IS 'account';
COMMENT ON COLUMN evt.bal.fspr IS 'period';
COMMENT ON COLUMN evt.bal.obal IS 'opening balance';
COMMENT ON COLUMN evt.bal.debits IS 'total debits';
COMMENT ON COLUMN evt.bal.credits IS 'total credits';
COMMENT ON COLUMN evt.bal.cbal IS 'closing balance';
COMMENT ON COLUMN evt.bal.prop IS 'json of period properties';


---------------------------handle new logged event----------------------------------------

CREATE OR REPLACE FUNCTION evt.log_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
    WITH
    --full extraction
    full_ex AS (
        SELECT
            ins.id
            --th econtents of the gl line
            ,a.i gl_line
            --the array position of the gl line
            ,a.rn gl_rownum
            --array of references
            ,ins.bpr#>ARRAY['gl','jpath',(a.rn - 1)::text] gl_ref 
            --each item in the reference array
            ,p.i ref_line
            --array postition of the reference item
            ,p.rn ref_rownum
            --follow the path
            ,ins.bpr#>(p.i->>0)::text[] bpr_extract
        FROM
            ins
            --gl array hold each gl line
            LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.bpr->'gl'->'lines') WITH ORDINALITY a(i, rn) ON TRUE
            --for each
            LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.bpr#>ARRAY['gl','jpath',(a.rn - 1)::text]) WITH ORDINALITY p(i, rn) ON TRUE
    )
    --select * from full_ex
    --re-ggregate extraction to gl line level
    ,ex_gl_line AS (
        SELECT 
            id
            ,(gl_line->>'account')::ltree account
            ,(gl_line->>'amount')::numeric amount
            ,gl_rownum
            --aggregate all the path references back to the gl line
            ,public.jsonb_concat(bpr_extract) bprkeys
        FROM 
            full_ex
        GROUP BY 
            id
            ,gl_line
            ,gl_rownum
    )
    --select * from ex_gl_line
    ,upsert_acct_mast AS (
        INSERT INTO
            evt.acct (acct,prop)
        SELECT DISTINCT
            account
            ,'{}'::jsonb prop
        FROM
            ex_gl_line
        ON CONFLICT DO NOTHING
        RETURNING *
    )
    INSERT INTO
        evt.gl (bprid,acct, amount,tstmp , fspr, glline, bprkeys)
    SELECT
        e.id
        ,e.account
        ,e.amount
        ,(e.bprkeys->>'date')::timestamptz
        ,p.id
        ,e.gl_rownum
        ,e.bprkeys
    FROM 
        ex_gl_line e
        LEFT OUTER JOIN evt.fspr p ON
            p.dur @> (bprkeys->>'date')::timestamptz;
    RETURN NULL;
    END;
    $func$;
    

CREATE TRIGGER log_insert 
    AFTER INSERT ON evt.bpr
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT 
    EXECUTE PROCEDURE evt.log_insert();

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
                ,cbal = evt.bal.cbal + EXCLUDED.debits + EXCLUDED.credits
            RETURNING *
        )
        ,seq AS (
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
                            ins
                            INNER JOIN evt.fspr f ON
                                f.id = ins.fspr
                            LEFT OUTER JOIN evt.bal b ON
                                b.acct = ins.acct
                            LEFT OUTER JOIN evt.fspr bp ON
                                bp.id = b.fspr
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

COMMIT;