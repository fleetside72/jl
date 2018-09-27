roll-forward needs intercepted by some retained earnings logic
    * top level period gets closed to retained earnings `subpath(id,0,1)`
    * top level account is a trial balance `subpath(acct,0,1)`

tasks
    * need to dynamically create a retained earnings account per trial balance if it does not exist
    * need to dynamically determine temporary accounts if not specified
    * a close-out to retained earning and consolidation process must happen before rolling into a new year, or opening balances need set to 0