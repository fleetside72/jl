------------------------fiscal periods------------------------
CREATE TABLE evt.fspr (
    id ltree
    ,dur tstzrange
)

CREATE INDEX fspr_id ON evt.fspr USING GIST (id);
COMMENT ON COLUMN evt.fspr.id IS 'fiscal period id';
COMMENT ON COLUMN evt.fspr.dur IS 'fiscal period dutation in timestamp range';