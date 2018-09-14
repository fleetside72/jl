CREATE FUNCTION log_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
    WITH
    ------------------------------------full extraction-------------------------------------------
    full_ex AS (
        SELECT
            ins.id
            ,a.i gl_line
            ,a.rn gl_row
            ,NEW.bpr#>ARRAY['gl','jpath',(a.rn - 1)::text] gl_ref
            ,p.i ref_line
            ,p.rn ref_row
            ,NEW.bpr#>(p.i->>0)::text[] bpr_extract
        FROM
            ins
            --gl array hold each gl line
            LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(NEW.bpr->'gl'->'lines') WITH ORDINALITY a(i, rn) ON TRUE
            --for each
            LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(NEW.bpr#>ARRAY['gl','jpath',(a.rn - 1)::text]) WITH ORDINALITY p(i, rn) ON TRUE
    )
    --------------------------------re-ggregate extraction to gl line level----------------------
    ,ex_gl_line
    SELECT 
        id
        ,gl_line->>'account' account
        ,(gl_line->>'amount')::numeric amount
        ,gl_row
        ,gl_ref
        ,public.jsonb_concat(bpr_extract) ref_extract
    FROM 
        full_ex
    GROUP BY 
        id
        ,gl_line
        ,gl_row
        ,gl_ref;
    RETURN NULL;
    END;
    $func$
    

CREATE TRIGGER log_insert 
    AFTER INSERT ON log 
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT 
    EXECUTE PROCEDURE log_insert();