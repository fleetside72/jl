--------------------------relational ledger------------------------------------------

CREATE TABLE evt.gl (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,bprid INT REFERENCES evt.bpr (id)
    ,acct text REFERENCES evt.acct (acct)
    ,pstmp timestamptz DEFAULT CURRENT_TIMESTAMP
    --populates by trigger join to evt.fspr
    ,tstmp timestamptz
    ,fspr ltree REFERENCES evt.fspr (id);
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