import asyncio

import pytest
from app.main import app
from app.ml.service import s3_service
from httpx import ASGITransport, AsyncClient


@pytest.fixture(autouse=True)
def mock_model():
    class DummyModel:
        def predict(self, X):
            return [100000.0 for _ in X]

    s3_service.model = DummyModel()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def ac():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client