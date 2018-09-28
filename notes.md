roll-forward needs intercepted by some retained earnings logic
* top level period gets closed to retained earnings `subpath(id,0,1)`
* top level account is a trial balance `subpath(acct,0,1)`

tasks
* a close-out to retained earning and consolidation process must happen before rolling into a new year, or opening balances need set to 0
    * test if rolling into a new year
    * need to dynamically create a retained earnings account per trial balance if it does not exist
    * need to dynamically determine temporary accounts if not specified
    

future
* work on UI and web server for different kinds of entries