BEGIN TRANSACTION;
--\conninfo
drop schema evt cascade;
--------------------------build schema----------------------------------------------

CREATE SCHEMA evt;
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
    acct text PRIMARY KEY
    ,prop jsonb
);
COMMENT ON COLUMN evt.acct.acct IS 'account';
COMMENT ON COLUMN evt.acct.prop IS 'properties';

--------------------------relational ledger------------------------------------------

CREATE TABLE evt.gl (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,bprid INT REFERENCES evt.bpr (id)
    ,acct text REFERENCES evt.acct (acct)
    ,amount numeric (12,2)
    ,glline INT
    ,bprkeys JSONB
);
COMMENT ON COLUMN evt.gl.id IS 'gl id';
COMMENT ON COLUMN evt.gl.bprid IS 'id of initial basic pecuniary record';
COMMENT ON COLUMN evt.gl.acct IS 'account code';
COMMENT ON COLUMN evt.gl.amount IS 'amount';
COMMENT ON COLUMN evt.gl.glline IS 'gl line number';
COMMENT ON COLUMN evt.gl.bprkeys IS 'extract from initial basic pecuniary record';

--------------------------balances----------------------------------------------------

CREATE TABLE evt.bal (
    acct TEXT REFERENCES evt.acct(acct)
    ,perd daterange
    ,obal numeric(12,2)
    ,debits numeric(12,2)
    ,credits numeric(12,2)
    ,cbal numeric(12,2)
);
COMMENT ON COLUMN evt.bal.acct IS 'account';
COMMENT ON COLUMN evt.bal.perd IS 'period';
COMMENT ON COLUMN evt.bal.obal IS 'opening balance';
COMMENT ON COLUMN evt.bal.debits IS 'total debits';
COMMENT ON COLUMN evt.bal.credits IS 'total credits';
COMMENT ON COLUMN evt.bal.cbal IS 'closing balance';

-----------------------------------------process bpr insert trigger -----------------------------------

CREATE OR REPLACE FUNCTION evt.log_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
    WITH
    ------------------------------------full extraction-------------------------------------------
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
    --------------------------------re-ggregate extraction to gl line level----------------------
    ,ex_gl_line AS (
        SELECT 
            id
            ,gl_line->>'account' account
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
        RETURNING *
    )
    INSERT INTO
        evt.gl (bprid,acct, amount,glline, bprkeys)
    SELECT
        id
        ,account
        ,amount
        ,gl_rownum
        ,bprkeys
    FROM 
        ex_gl_line;
    RETURN NULL;
    END;
    $func$;
    

CREATE TRIGGER log_insert 
    AFTER INSERT ON evt.bpr
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT 
    EXECUTE PROCEDURE evt.log_insert();


COMMIT;
