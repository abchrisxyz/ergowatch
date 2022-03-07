import requests
import pytest

from fixtures import balances_fork_env
from utils import run_watcher


def test_forked_chain_is_rolled_back(balances_fork_env):
    """
    Test watcher rolls back forked blocks
    """
    db_conn, cfg_path = balances_fork_env

    r = requests.get(f"http://localhost:9053/enable_stepping")
    assert r.status_code == 200

    # First run. This will include block 698626_fork
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    with db_conn.cursor() as cur:
        # Check headers
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [698_625, 698_626]
        # Header id's
        assert [r[1] for r in rows] == [
            "aaf522f0d258fc3641a19f89e04a59f29d1b09f7c54c8788d4c0ba343b6f3a84",
            "d674dcddda1e50330b9375397457a590ab91a162bb81d4ae81a8e4e0b87db7ba",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [698_625, 698_626]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "ea87bf019d0bbe49d662d3d9eace53c12bfd37a3472af6b159d4edcfab5a6afd",
        ]

        # Check balances
        # Note: box for Gxd4hMRT got spent, balance dropped to zero and hence not present in erg.bal anymore.
        cur.execute("select value, address from bal.erg order by value;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows[0] == (
            1100000,
            "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
        )
        assert rows[1] == (
            3900000,
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b",
        )
        assert rows[2] == (
            5000000,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
        )
        assert rows[3] == (
            16531500000,
            "8D6pdYVRvF6NBMS9eYgaxuyGYuuw58StGfbMLkXFjHsYtZ9GpLPth4iRmKVoQxfPBKiAQ6en5igfu8xyZBEcnNJzYF2hPcX8tVJStz6XxrxeCW35RsWbgAui1VNXtviP2P9ARjwrEHsuSqLZCqSGRzvDejFzQ22fyD3NhiofrAUw8gqECinSmXMWcx1b2bdfe6NRL1u2znPwKphwmxhPqw8qK8VnWBNEtRi5rAHu4yHnMRCxG6GXmKHR2qmjc2JApHqFfDs3wSVT5hiQdGUXyZsaKNzrzQ2MCfWi1s2JmCDL34oXdyepxih3koCVSs3AqmWeUjrSNyEUeBXUThytw68Y4rERuboxj5bmWUBvE5GDN62ErYzad1MuiGimGYk85Za4e7gR73qNHaDEd3Eqf55Cpkd7bB2DY2VoLSpt2Ns2JEkVSfpAQwEWtuyyGPbyBzTXunKjEEF7eAops65MSBMT1WBnMNyF6oR5aLVbk7Xi9sQgXuZ3bVUVmepVMaYcwmEta86eDmyprG7DSyJTz3df7nrisN5qm8P7itfzZ16VETkiqQeZstiRQB1bx",
        )
        assert rows[4] == (
            93000000000000000,
            "dummy-address-0",
        )

        # Check diffs
        cur.execute(
            "select height, value, address, tx_id from bal.erg_diffs where address = 'Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR' order by 1 ;"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            698620,
            16531500000,
            "Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR",
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
        )
        assert rows[1] == (
            698626,
            -16531500000,
            "Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR",
            "ea87bf019d0bbe49d662d3d9eace53c12bfd37a3472af6b159d4edcfab5a6afd",
        )

        cur.execute(
            "select height, value, address, tx_id from bal.erg_diffs where address = '9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr' order by 1 ;"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            698620,
            5000000 * 2,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
        )
        assert rows[1] == (
            698626,
            -5000000,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
            "ea87bf019d0bbe49d662d3d9eace53c12bfd37a3472af6b159d4edcfab5a6afd",
        )

    # Step
    r = requests.get(f"http://localhost:9053/step")
    assert r.status_code == 200

    # Second run. Should change nothing as node is still at height 698626.
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    with db_conn.cursor() as cur:
        # Check headers
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [698_625, 698_626]
        # Header id's
        assert [r[1] for r in rows] == [
            "aaf522f0d258fc3641a19f89e04a59f29d1b09f7c54c8788d4c0ba343b6f3a84",
            "d674dcddda1e50330b9375397457a590ab91a162bb81d4ae81a8e4e0b87db7ba",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [698_625, 698_626]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "ea87bf019d0bbe49d662d3d9eace53c12bfd37a3472af6b159d4edcfab5a6afd",
        ]

        # Check balances
        # Note: box for Gxd4hMRT got spent, balance dropped to zero and hence not present in erg.bal anymore.
        cur.execute("select value, address from bal.erg order by value;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows[0] == (
            1100000,
            "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
        )
        assert rows[1] == (
            3900000,
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b",
        )
        assert rows[2] == (
            5000000,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
        )
        assert rows[3] == (
            16531500000,
            "8D6pdYVRvF6NBMS9eYgaxuyGYuuw58StGfbMLkXFjHsYtZ9GpLPth4iRmKVoQxfPBKiAQ6en5igfu8xyZBEcnNJzYF2hPcX8tVJStz6XxrxeCW35RsWbgAui1VNXtviP2P9ARjwrEHsuSqLZCqSGRzvDejFzQ22fyD3NhiofrAUw8gqECinSmXMWcx1b2bdfe6NRL1u2znPwKphwmxhPqw8qK8VnWBNEtRi5rAHu4yHnMRCxG6GXmKHR2qmjc2JApHqFfDs3wSVT5hiQdGUXyZsaKNzrzQ2MCfWi1s2JmCDL34oXdyepxih3koCVSs3AqmWeUjrSNyEUeBXUThytw68Y4rERuboxj5bmWUBvE5GDN62ErYzad1MuiGimGYk85Za4e7gR73qNHaDEd3Eqf55Cpkd7bB2DY2VoLSpt2Ns2JEkVSfpAQwEWtuyyGPbyBzTXunKjEEF7eAops65MSBMT1WBnMNyF6oR5aLVbk7Xi9sQgXuZ3bVUVmepVMaYcwmEta86eDmyprG7DSyJTz3df7nrisN5qm8P7itfzZ16VETkiqQeZstiRQB1bx",
        )
        assert rows[4] == (
            93000000000000000,
            "dummy-address-0",
        )

        # Check diffs
        cur.execute(
            "select height, value, address, tx_id from bal.erg_diffs where address = 'Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR' order by 1 ;"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            698620,
            16531500000,
            "Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR",
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
        )
        assert rows[1] == (
            698626,
            -16531500000,
            "Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR",
            "ea87bf019d0bbe49d662d3d9eace53c12bfd37a3472af6b159d4edcfab5a6afd",
        )

        cur.execute(
            "select height, value, address, tx_id from bal.erg_diffs where address = '9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr' order by 1 ;"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            698620,
            5000000 * 2,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
        )
        assert rows[1] == (
            698626,
            -5000000,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
            "ea87bf019d0bbe49d662d3d9eace53c12bfd37a3472af6b159d4edcfab5a6afd",
        )

    # Step
    r = requests.get(f"http://localhost:9053/step")
    assert r.status_code == 200

    # Third run. Should roll back 698626_fork, include 698626 instead, and then 698627.
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    with db_conn.cursor() as cur:
        # Check headers
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [698_625, 698_626, 698_627]
        # Header id's
        assert [r[1] for r in rows] == [
            "aaf522f0d258fc3641a19f89e04a59f29d1b09f7c54c8788d4c0ba343b6f3a84",
            "dc7e5c2b3f365b6844ba3c0beac482b4659e72fdb8097c9729872566f7c24d12",
            "9f01391f5b76e9b15fe9ef3f58fd5ff13ddcb07f43a58d91ad7e5a0885617f60",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [698_625, 698_626]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "ea87bf-from-main-chain-block",
        ]

        # Check balances
        cur.execute("select value, address from bal.erg order by value;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows[0] == (
            1100000,
            "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
        )
        assert rows[1] == (
            3900000,
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b",
        )
        assert rows[2] == (
            5000000,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
        )
        assert rows[3] == (
            16531500000,
            "8D6pdYVRvF6NBMS9eYgaxuyGYuuw58StGfbMLkXFjHsYtZ9GpLPth4iRmKVoQxfPBKiAQ6en5igfu8xyZBEcnNJzYF2hPcX8tVJStz6XxrxeCW35RsWbgAui1VNXtviP2P9ARjwrEHsuSqLZCqSGRzvDejFzQ22fyD3NhiofrAUw8gqECinSmXMWcx1b2bdfe6NRL1u2znPwKphwmxhPqw8qK8VnWBNEtRi5rAHu4yHnMRCxG6GXmKHR2qmjc2JApHqFfDs3wSVT5hiQdGUXyZsaKNzrzQ2MCfWi1s2JmCDL34oXdyepxih3koCVSs3AqmWeUjrSNyEUeBXUThytw68Y4rERuboxj5bmWUBvE5GDN62ErYzad1MuiGimGYk85Za4e7gR73qNHaDEd3Eqf55Cpkd7bB2DY2VoLSpt2Ns2JEkVSfpAQwEWtuyyGPbyBzTXunKjEEF7eAops65MSBMT1WBnMNyF6oR5aLVbk7Xi9sQgXuZ3bVUVmepVMaYcwmEta86eDmyprG7DSyJTz3df7nrisN5qm8P7itfzZ16VETkiqQeZstiRQB1bx",
        )
        assert rows[4] == (
            93000000000000000,
            "dummy-address-0",
        )

        # Check diffs
        cur.execute(
            "select height, value, address, tx_id from bal.erg_diffs where address = 'Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR' order by 1 ;"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            698620,
            16531500000,
            "Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR",
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
        )
        assert rows[1] == (
            698626,
            -16531500000,
            "Gxd4hMRT6Lbs5aptyP3Ad2rB5FQD1SbxAWZacB7JLsMiMyRa1zcpKCLnsbXkmnE9NoHLZUTMYSvRh6eLXRKStcQVPNeVkeix5PKbh4z77KZepfnYPpU8weXkxuVfYudo4LwumcwV7uUsoxdop7j35MfZByYndqCZZ3UrqkCmViZdHKexRmDnpRpijMUELKopRJW8LMa2aVrw71W4gL3R3TrvdrPxzEGYj2EQHS8T1ZBKnskrDGBpYgqJ3xTb52HSjsLw81zRwXnWqn51zD4njtjVKeK4xwKePLhzK1e4ggfLEubMiHtBMXY3vngrTLsvrpfw1Bzuu2Edp2LR7mT46rFW2AzrStQvhCmCzDhjdbJra6ikg2FdzN4o4qUZ6GTWKzesGVcYynRbYR",
            "ea87bf-from-main-chain-block",
        )

        cur.execute(
            "select height, value, address, tx_id from bal.erg_diffs where address = '9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr' order by 1 ;"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            698620,
            5000000 * 2,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
        )
        assert rows[1] == (
            698626,
            -5000000,
            "9hEZyV3xCHVqx6SzD9GiL8wFMu4WKSodeakK3uyX3KQ1rKKkSDr",
            "ea87bf-from-main-chain-block",
        )
