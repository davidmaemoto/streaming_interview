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

def test_samples_yield_nothing():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 20.0},
        {"type": "sample", "stationName": "B", "timestamp": 3, "temperature": 15.0},
    ]
    result = list(weather.process_events(events))
    assert result == []
