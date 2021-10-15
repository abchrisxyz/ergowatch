## Upgrading postgres containers using pg_upgrade

#### 1. Use version numbered directories

`pg_upgrade` requires numbered directories **under the same root**, something like:

```
/path/to/data/13/data
/path/to/data/14/data
```

If the db container is currently using a named volume, shutdown the container, move the data to a "native" directory including postgresql's main version e.g.:

```
mv /var/lib/docker/volumes/ew_postgres/_data /var/lib/ergowatch_pg/13/data
```

Delete the docker volume.

Restart the container with updated mount options pointing to the new data dir, and check db is working fine.

#### 2. Shutdown current db container

With docker-compose, use `stop`, then `down`, to ensure a clean termination of the db cluster.

Using `down` only can make `pg_upgrade` complain:

```
The source cluster was not shut down cleanly.
Failure, exiting.
```

#### 3. Initialize the new cluster

Start the db service defined in `docker-compose-pg-upgrade.yml` and then stop it again.

This is just to ensure the new db has been initialized with whatever users we're using.

```
docker-compose -f docker-compose-pg-upgrade.yml up -d --build
docker-compose -f docker-compose-pg-upgrade.yml stop db
docker-compose -f docker-compose-pg-upgrade.yml down
```

#### 4. Run pg_upgrade

This done using https://github.com/tianon/docker-postgres-upgrade 

```
docker run --rm --env PGUSER=ergo --env PGPORTOLD=5433  --env PGPORTNEW=5434 -v /var/lib/ergowatch_pg:/var/lib/postgresql tianon/postgres-upgrade:13-to-14 --link
```

#### 5. Update `docker-compose.yml` and db Dockerfile

Upgrade postgres version in `./explorer-backend/db/Dockerfile` and volume path in `./docker-compose.yml`

#### 6. Post upgrade actions

```
Upgrade Complete
----------------
Optimizer statistics are not transferred by pg_upgrade.
Once you start the new server, consider running:
    /usr/lib/postgresql/14/bin/vacuumdb --all --analyze-in-stages

Running this script will delete the old cluster's data files:
    ./delete_old_cluster.sh
```
