
CREATE FUNCTION log_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
    WITH
    /*
    ins AS (
        SELECT
            1 id
            ,$${
    "gl": {
        "lines": [
            {
                "amount": 2.19,
                "account": "h.food"
            },
            {
                "amount": -2.19,
                "account": "h.dcard"
            }
        ],
        "jpath": [
            [
                "{item,0}",
                "{header}"
            ],
            [
                "{item,0}",
                "{header}"
            ]
        ]
    },
    "item": [
        {
            "item": "green olives",
            "amount": 2.19,
            "reason": "food",
            "account": "h.food"
        }
    ],
    "header": {
        "entity": "home",
        "module": "MHI",
        "offset": "h.dcard",
        "transaction": "purchase"
    }
}$$::jsonb bpr
    ),
    */
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
    INSERT INTO
        evt.gl (bprid,account, amount,glline, bprkeys)
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
    $func$
    

CREATE TRIGGER log_insert 
    AFTER INSERT ON evt.log 
    REFERENCING NEW TABLE AS ins 
    FOR EACH STATEMENT 
    EXECUTE PROCEDURE evt.log_insert();