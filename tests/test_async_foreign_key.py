import pytest

from turso_python.async_connection import AsyncTursoConnection
from turso_python.async_crud import AsyncTursoCRUD


@pytest.fixture
async def async_crud_client():
    async with AsyncTursoConnection() as conn:
        crud = AsyncTursoCRUD(conn)
        yield crud


@pytest.mark.asyncio
async def test_foreign_key_checks(async_crud_client: AsyncTursoCRUD):
    """Self-contained FK test: creates dedicated tables with an explicit FK, then toggles PRAGMA."""
    # Check initial status
    initial_status = await async_crud_client.get_foreign_key_checks_status()
    
    # Ensure foreign key checks are enabled for testing
    await async_crud_client.set_foreign_key_checks(True)

    # Prepare isolated test tables (drop if exist just in case)
    await async_crud_client.connection.execute_query("DROP TABLE IF EXISTS tokens_fk_test;")
    await async_crud_client.connection.execute_query("DROP TABLE IF EXISTS users_fk_test;")

    await async_crud_client.connection.execute_query(
        """
        CREATE TABLE IF NOT EXISTS users_fk_test (
            uid TEXT PRIMARY KEY
        );
        """
    )
    await async_crud_client.connection.execute_query(
        """
        CREATE TABLE IF NOT EXISTS tokens_fk_test (
            token_hash TEXT PRIMARY KEY,
            uid TEXT,
            encrypted_data TEXT,
            created_at INTEGER,
            expires_at INTEGER,
            FOREIGN KEY(uid) REFERENCES users_fk_test(uid)
        );
        """
    )

    # Sanity: verify PRAGMA is on
    status_on = await async_crud_client.get_foreign_key_checks_status()
    assert status_on is True, "Failed to enable foreign key checks"

    # Create a test user
    await async_crud_client.create("users_fk_test", {"uid": "TEST_USER_999"})

    # This should succeed - create token with valid uid
    await async_crud_client.create(
        "tokens_fk_test",
        {
            "token_hash": "valid_token_hash_test",
            "uid": "TEST_USER_999",
            "encrypted_data": "test_encrypted_data",
            "created_at": 1234567890,
            "expires_at": 1234567999,
        },
    )

    # This should fail because uid "TEST_USER_998" does not exist in users_fk_test
    with pytest.raises(Exception) as excinfo:
        await async_crud_client.create(
            "tokens_fk_test",
            {
                "token_hash": "invalid_user_token_hash",
                "uid": "TEST_USER_998",
                "encrypted_data": "test_encrypted_data_2",
                "created_at": 1234567890,
                "expires_at": 1234567999,
            },
        )
    assert "FOREIGN KEY" in str(excinfo.value).upper()

    # Disable foreign key checks
    await async_crud_client.set_foreign_key_checks(False)

    # Verify they are off
    status_off = await async_crud_client.get_foreign_key_checks_status()
    assert status_off is False, "Failed to disable foreign key checks"

    # This should now succeed even with the invalid foreign key
    await async_crud_client.create(
        "tokens_fk_test",
        {
            "token_hash": "should_work_with_fk_off_hash",
            "uid": "TEST_USER_998",
            "encrypted_data": "test_encrypted_data_3",
            "created_at": 1234567890,
            "expires_at": 1234567999,
        },
    )

    # Cleanup: drop test tables and restore initial PRAGMA
    try:
        await async_crud_client.connection.execute_query("DROP TABLE IF EXISTS tokens_fk_test;")
        await async_crud_client.connection.execute_query("DROP TABLE IF EXISTS users_fk_test;")
    finally:
        await async_crud_client.set_foreign_key_checks(initial_status)
