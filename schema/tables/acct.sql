--------------------------account master---------------------------------------------

--the account master should be dynamically created
CREATE TABLE evt.acct (
    acct ltree PRIMARY KEY
    ,prop jsonb
);
COMMENT ON TABLE evt.acct IS 'account master list';
COMMENT ON COLUMN evt.acct.acct IS 'account';
COMMENT ON COLUMN evt.acct.prop IS 'properties';

--this should effectively only allow one instance of an account where retained_earnings = set per top level account (trial balance)
CREATE UNIQUE INDEX acct_re ON evt.acct (subpath(acct,0,1)) WHERE prop ->> 'retained_earnings' = 'set';
