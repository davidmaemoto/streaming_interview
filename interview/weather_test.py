import pytest
from . import weather

def test_basic_message_validation():
    events = [{"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0}]
    result = list(weather.process_events(events))
    assert result == []

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

def test_sample_missing_fields():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1},
        {"type": "sample", "stationName": "A", "temperature": 10.0},
        {"type": "sample", "timestamp": 1, "temperature": 10.0},
    ]
    for event in events:
        with pytest.raises(ValueError, match="Please verify input. Sample must contain stationName, timestamp, and temperature."):
            list(weather.process_events([event]))

def test_snapshot_output():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 20.0},
        {"type": "sample", "stationName": "B", "timestamp": 3, "temperature": 15.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 1
    snap = result[0]
    assert snap["type"] == "snapshot"
    assert snap["asOf"] == 3
    assert snap["stations"] == {
        "A": {"high": 20.0, "low": 10.0},
        "B": {"high": 15.0, "low": 15.0},
    }
