--------------------------relational ledger------------------------------------------

CREATE TABLE evt.gl (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,bpr_id INT REFERENCES evt.bpr (id)
    ,acct text REFERENCES evt.acct (acct)
    ,amount numeric (12,2)
    ,gl_line INT
    ,bpr_extract JSONB
);
COMMENT ON COLUMN evt.gl.bpr_id IS 'id of initial basic pecuniary record';
COMMENT ON COLUMN evt.gl.acct IS 'account code';
COMMENT ON COLUMN evt.gl.amount IS 'amount';
COMMENT ON COLUMN evt.gl.bpr IS 'extract from initial basic pecuniary record';