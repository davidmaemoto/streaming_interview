import pytest
from . import weather

def test_basic_message_validation():
    events = [{"type": "sample", "data": "test"}]
    result = list(weather.process_events(events))
    assert result == events

def test_unknown_message_type():
    events = [{"type": "unknown", "data": "test"}]
    with pytest.raises(ValueError, match="Please verify input. Unknown message type: unknown"):
        list(weather.process_events(events))

def test_missing_type_field():
    events = [{"data": "test"}]
    with pytest.raises(ValueError, match="Please verify input. Event must be a dictionary with 'type' field."):
        list(weather.process_events(events))
