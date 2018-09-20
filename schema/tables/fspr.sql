------------------------fiscal periods------------------------
CREATE TABLE evt.fspr (
    id ltree
    ,dur tstzrange
)


CREATE INDEX id_gist ON evt.fspr USING GIST (id);