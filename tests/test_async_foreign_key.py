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
    """Test enabling and disabling foreign key checks using existing users and tokens tables."""
    # Check initial status
    initial_status = await async_crud_client.get_foreign_key_checks_status()
    print(f"Initial foreign key status: {initial_status}")
    
    # Ensure foreign key checks are enabled for testing
    await async_crud_client.set_foreign_key_checks(True)
    
    # Verify they are on
    status_on = await async_crud_client.get_foreign_key_checks_status()
    assert status_on is True, "Failed to enable foreign key checks"
    print(f"Foreign key checks enabled: {status_on}")
    
    # Clean up any existing test data first
    try:
        await async_crud_client.delete("tokens", "uid = ?", ["TEST_USER_999"])
        await async_crud_client.delete("users", "uid = ?", ["TEST_USER_999"])
        await async_crud_client.delete("users", "uid = ?", ["TEST_USER_998"])
    except Exception:
        pass  # Ignore errors if records don't exist
    
    # Create a test user
    await async_crud_client.create("users", {"uid": "TEST_USER_999"})
    
    # This should succeed - create token with valid uid
    await async_crud_client.create("tokens", {
        "token_hash": "valid_token_hash_test",
        "uid": "TEST_USER_999", 
        "encrypted_data": "test_encrypted_data",
        "created_at": 1234567890,
        "expires_at": 1234567999
    })
    
    # This should fail because uid "TEST_USER_998" does not exist in users table
    try:
        await async_crud_client.create("tokens", {
            "token_hash": "invalid_user_token_hash",
            "uid": "TEST_USER_998", 
            "encrypted_data": "test_encrypted_data_2",
            "created_at": 1234567890,
            "expires_at": 1234567999
        })
        pytest.fail("Expected an exception when inserting token with invalid uid")
    except Exception as e:
        print(f"Expected foreign key error: {e}")
        # Check if it's actually a foreign key constraint error
        assert "FOREIGN KEY constraint failed" in str(e), f"Expected foreign key error, got: {e}"
    
    # Disable foreign key checks
    await async_crud_client.set_foreign_key_checks(False)
    
    # Verify they are off
    status_off = await async_crud_client.get_foreign_key_checks_status()
    assert status_off is False, "Failed to disable foreign key checks"
    print(f"Foreign key checks disabled: {status_off}")
    
    # This should now succeed even with the invalid foreign key
    await async_crud_client.create("tokens", {
        "token_hash": "should_work_with_fk_off_hash",
        "uid": "TEST_USER_998", 
        "encrypted_data": "test_encrypted_data_3",
        "created_at": 1234567890,
        "expires_at": 1234567999
    })
    
    # Clean up test data
    try:
        await async_crud_client.delete("tokens", "uid IN (?, ?)", ["TEST_USER_999", "TEST_USER_998"])
        await async_crud_client.delete("users", "uid = ?", ["TEST_USER_999"])
    except Exception:
        pass
    
    # Restore initial foreign key status
    await async_crud_client.set_foreign_key_checks(initial_status)