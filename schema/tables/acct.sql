--------------------------account master---------------------------------------------

--the account master should be dynamically created
CREATE TABLE evt.acct (
    acct ltree PRIMARY KEY
    ,prop jsonb
);
COMMENT ON COLUMN evt.acct.acct IS 'account';
COMMENT ON COLUMN evt.acct.prop IS 'properties';