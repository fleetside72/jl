--------------------------balances----------------------------------------------------

CREATE TABLE evt.bal (
    acct ltree REFERENCES evt.acct(acct)
    ,fspr ltree REFERENCES evt.fspr(id)
    ,obal numeric(12,2)
    ,debits numeric(12,2)
    ,credits numeric(12,2)
    ,cbal numeric(12,2)
);
ALTER TABLE evt.bal ADD CONSTRAINT bal_pk PRIMARY KEY(acct,fspr);
COMMENT ON COLUMN evt.bal.acct IS 'account';
COMMENT ON COLUMN evt.bal.fspr IS 'period';
COMMENT ON COLUMN evt.bal.obal IS 'opening balance';
COMMENT ON COLUMN evt.bal.debits IS 'total debits';
COMMENT ON COLUMN evt.bal.credits IS 'total credits';
COMMENT ON COLUMN evt.bal.cbal IS 'closing balance';
