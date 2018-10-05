roll-forward needs intercepted by some retained earnings logici
* top level period gets closed to retained earnings `subpath(id,0,1)`
* top level account is a trial balance `subpath(acct,0,1)`

tasks
* a close-out to retained earning and consolidation process must happen before rolling into a new year, or opening balances need set to 0
    * test if rolling into a new year
        * copy the last period from evt.bal _after rolled_  `2018.12` and copy to `2018.close`
        * close move temporary account balances to retained earnings
            * need to dynamically create a retained earnings account per trial balance if it does not exist
            * need to dynamically determine temporary accounts if not specified
        * copy `2018.close` ending balances to `2019.01` opening balances and re-roll into the new year
    

future
* work on UI and web server for different kinds of entries

accounts that need labeled
* asset
* liability
* equity
    * py re
    * cy re
    * oci
    * cta
    * apic (hold at acquisition rate)
    * common stock (hold at acquisition rate)
    * dividends

how to assign
* ~~as part of hierarchy~~
* as a property

when to assign
* as required (notify at close that accounts labeled as earnings temporary will be closed to retained earnings)
* any balance sheet offset accounts must be considered equity in order to print a coherent balance sheet
