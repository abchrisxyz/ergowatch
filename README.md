# ErgoWatch
Ergo blockchain stats & monitoring.

The backend is built on top of [Ergo Explorer](https://github.com/ergoplatform/explorer-backend)'s chain-grabber and database:

 - An ErgoWatch specific schema, [ew.sql](https://github.com/abchrisxyz/ergowatch/blob/master/explorer-backend/db/ew.sql) is added to the database.
 - DB emits a notification each time a new height is written to it (by the chain-grabber).
 - The [syncer](https://github.com/abchrisxyz/ergowatch/tree/master/syncer) listens for db notifications and triggers updates of ErgoWatch relations.
 - ErgoWatch relations exposed through FastAPI instance.
 
Frontend: see https://github.com/abchrisxyz/ergowatch-ui
 
