---------------------------handle new logged event----------------------------------------

CREATE OR REPLACE FUNCTION evt.log_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS 
    $func$
    BEGIN
        DELETE
        FROM
            evt.gl g
        WHERE EXISTS
        (
            SELECT
                NULL::int
            FROM
                g
                INNER JOIN del ON
                    del.id = g.bprid
        );
    RETURN NULL;
    END;
    $func$;
    
COMMENT ON FUNCTION evt.log_delete IS 'perspective lines assocated with deleted event';

CREATE TRIGGER log_delete
    AFTER DELETE ON evt.bpr
    REFERENCING OLD TABLE AS del
    FOR EACH STATEMENT 
    EXECUTE PROCEDURE evt.log_delete();