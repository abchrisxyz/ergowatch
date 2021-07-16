import os
import asyncio
import asyncpg
import time
import logging


# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info("Retrieving DB settings from environment")
try:
    DB_HOST = os.environ["DB_URL"]
    DB_NAME = os.environ["DB_NAME"]
    DB_USER = os.environ["DB_USER"]
    DB_PASS = os.environ["POSTGRES_PASSWORD"]
except KeyError as e:
    logger.error(f"Environment variable {e} is not set")
    exit(1)

DBSTR = f"postgresql://{DB_PASS}:{DB_USER}@{DB_HOST}/{DB_NAME}"

# Node heights queue
Q = asyncio.Queue()

# Using a queue as some sort of mutex for our single db connection
C = asyncio.Queue()


def handle_notification(conn, pid, channel, payload):
    logger.info(f"Received notificatoin with payload: {payload}")
    Q.put_nowait(payload)
    logger.info(f"Queue size is now: {Q.qsize()}")


def process_queue():
    while Q.qsize() > 1:
        height = Q.get_nowait()
        logger.info(f"Skipping height: {height} - more recent one available")

    height = Q.get_nowait()
    conn = C.get_nowait()

    # Wait some to ensure chain-grabber is done
    time.sleep(2)

    async def refresh():
        try:
            await conn.execute("CALL ew.sync($1);", int(height))
            logger.info(f"Task for {height} completed")
        except asyncpg.InterfaceError as e:
            logger.warning(e)
            logger.warning(f"Aborting task for {height}")
        finally:
            # "release" connection by putting it back in the queue
            C.put_nowait(conn)

    logger.info(f"Submitting task for {height}")
    asyncio.create_task(refresh())


async def main():
    logger.info(f"Connecting to database: {DB_USER}@{DB_HOST}/{DB_NAME}")
    conn = await asyncpg.connect(DBSTR)

    channel = "ergowatch"
    logger.info(f"Adding listener on channel '{channel}'")
    await conn.add_listener(channel, handle_notification)

    C.put_nowait(conn)

    while True:
        if not Q.empty() and not C.empty():
            # There is at least one new height to process
            # and db is done processing previous ones.
            process_queue()
        await asyncio.sleep(10)

    logger.info("Closing db connection")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
