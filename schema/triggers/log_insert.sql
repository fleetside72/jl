
WITH
    NEW as (
        SELECT $${
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
    )
SELECT
    *
FROM
    NEW
    --gl array hold each gl line
    LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(NEW.bpr->'gl') WITH ORDINALITY gl(i, rn) ON TRUE
    --eaxpand the array of gl lines
    LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(gl.i->'lines') WITH ORDINALITY a(i, rn) ON TRUE
    --for each
    LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(gl.i->'jpath') WITH ORDINALITY p(i, rn) ON TRUE