# ErgoWatch
Ergo blockchain stats & monitoring.

The backend is built on top of [Ergo Explorer](https://github.com/ergoplatform/explorer-backend)'s chain-grabber and database:

 - ErgoWatch specific schemas are defined in [db](https://github.com/abchrisxyz/ergowatch/tree/master/db).
 - DB emits a notification each time a new height is written to it (by the chain-grabber).
 - The [syncer](https://github.com/abchrisxyz/ergowatch/tree/master/syncer) listens for db notifications and triggers updates of ErgoWatch relations.
 - ErgoWatch relations are exposed through a FastAPI layer.

Frontend: see https://github.com/abchrisxyz/ergowatch-ui

## Installation

```bash
git clone https://github.com/abchrisxyz/ergowatch
cd ergowatch

# install grabber
./build.sh

# create Postgres database password file
touch ./explorer-backend/db/db.secret

# update newly created Postgress database password file with:
POSTGRES_PASSWORD=your-database-password

# Create  external docker volume for ergowatch
docker volume create ew_node

# Build and run the docker environement
docker-compose -f docker-compose.yml up -d --build
```

> Please note that you might see errors in your docker logs but these will automatically
> disappear once your node has reached a `height` value. To check height status, 
> browse to either
> [http://localhost:9053/info](http://localhost:9053/info)
> or 
> [http://localhost:9053/panel](http://localhost:9053/panel).

## Database

Connect using any PostgreSQL client and specifying:

- database `ergo`
- port `5433`
- the password you configured earlier
