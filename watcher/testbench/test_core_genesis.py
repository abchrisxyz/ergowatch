from fixtures import blank_db
from api import mock_api_genesis


def test_first_block(blank_db, mock_api_genesis):
    """
    Check connection works and db is blank
    """
    # TODO:
    #   - pass db settings to watcher
    #   - add option to watcher to exit at sync

    # Run watcher

    # Read db to verify state
    with blank_db.cursor() as cur:
        pass
