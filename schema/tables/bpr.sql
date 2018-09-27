--------------------------event log table-------------------------------------------

CREATE TABLE evt.bpr (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,bpr JSONB
    ,bprh JSONB
    ,stmp timestamptz
);
COMMENT ON TABLE evt.bpr IS 'log of events';
COMMENT ON COLUMN evt.bpr.bpr IS 'basic pecuniary record';
COMMENT ON COLUMN evt.bpr.bprh IS 'history';
COMMENT ON COLUMN evt.bpr.stmp IS 'insert time';