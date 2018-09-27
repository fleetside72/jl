------------------------fiscal periods------------------------
CREATE TABLE evt.fspr (
    id ltree PRIMARY KEY
    ,dur tstzrange
    ,prop jsonb
);
COMMENT ON TABLE evt.fspr IS 'fiscal period definitions';
COMMENT ON COLUMN evt.fspr.id IS 'fiscal period';
COMMENT ON COLUMN evt.fspr.dur IS 'duration of period as timestamp range';
COMMENT ON COLUMN evt.fspr.prop IS 'period properties';
CREATE INDEX id_gist ON evt.fspr USING GIST (id);
