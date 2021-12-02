# ErgoWatch
Ergo blockchain stats & monitoring.

The backend is built on top of [Ergo Explorer](https://github.com/ergoplatform/explorer-backend)'s chain-grabber and database:

 - ErgoWatch specific schemas are defined in [db](https://github.com/abchrisxyz/ergowatch/tree/master/db).
 - DB emits a notification each time a new height is written to it (by the chain-grabber).
 - The [syncer](https://github.com/abchrisxyz/ergowatch/tree/master/syncer) listens for db notifications and triggers updates of ErgoWatch relations.
 - ErgoWatch relations are exposed through a FastAPI layer.

Frontend: see https://github.com/abchrisxyz/ergowatch-ui
