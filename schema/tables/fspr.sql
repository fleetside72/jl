------------------------fiscal periods------------------------
CREATE TABLE evt.fspr (
    id ltree PRIMARY KEY
    ,dur tstzrange
);

COMMENT ON COLUMN evt.fspr.id IS 'fiscal period';
COMMENT ON COLUMN evt.fspr.dur IS 'duration of period as timestamp range';
CREATE INDEX id_gist ON evt.fspr USING GIST (id);
