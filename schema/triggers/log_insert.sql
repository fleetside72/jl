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