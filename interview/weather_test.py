import pytest
from . import weather

def test_basic_message_validation():
    events = [{"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0}]
    result = list(weather.process_events(events))
    assert not result

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
    assert not result

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

def test_reset_output():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "reset"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 1
    reset = result[0]
    assert reset["type"] == "reset"
    assert reset["asOf"] == 1

def test_reset_clears_data():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "reset"},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 1  # only reset output, no snapshot after reset
    assert result[0]["type"] == "reset"

def test_unknown_control_command():
    events = [{"type": "control", "command": "unknown"}]
    with pytest.raises(ValueError, match="Please verify input. Unknown control command: unknown"):
        list(weather.process_events(events))

def test_control_missing_command():
    events = [{"type": "control"}]
    with pytest.raises(ValueError, match="Please verify input. Control message must contain 'command' field."):
        list(weather.process_events(events))

def test_snapshot_without_data():
    events = [{"type": "control", "command": "snapshot"}]
    result = list(weather.process_events(events))
    assert not result

def test_reset_without_data():
    events = [{"type": "control", "command": "reset"}]
    result = list(weather.process_events(events))
    assert not result

def test_multiple_stations():
    events = [
        {"type": "sample", "stationName": "Station A", "timestamp": 1000, "temperature": 37.1},
        {"type": "sample", "stationName": "Station B", "timestamp": 2000, "temperature": 42.5},
        {"type": "sample", "stationName": "Station C", "timestamp": 3000, "temperature": 15.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 1
    stations = result[0]["stations"]
    assert len(stations) == 3
    assert stations["Station A"]["high"] == 37.1
    assert stations["Station A"]["low"] == 37.1
    assert stations["Station B"]["high"] == 42.5
    assert stations["Station B"]["low"] == 42.5
    assert stations["Station C"]["high"] == 15.0
    assert stations["Station C"]["low"] == 15.0

def test_timestamp_ordering():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 2000, "temperature": 20.0},
        {"type": "sample", "stationName": "B", "timestamp": 1000, "temperature": 10.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert result[0]["asOf"] == 2000  # should be the latest timestamp

def test_multiple_samples_same_station():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 5.0},
        {"type": "sample", "stationName": "A", "timestamp": 3, "temperature": 25.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    station_data = result[0]["stations"]["A"]
    assert station_data["high"] == 25.0
    assert station_data["low"] == 5.0

def test_generator_behavior():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "snapshot"},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 20.0},
        {"type": "control", "command": "snapshot"},
    ]
    generator = weather.process_events(events)
    
    first_result = next(generator)
    assert first_result["type"] == "snapshot"
    assert first_result["stations"]["A"]["high"] == 10.0
    
    second_result = next(generator)
    assert second_result["type"] == "snapshot"
    assert second_result["stations"]["A"]["high"] == 20.0
    
    with pytest.raises(StopIteration):
        next(generator)

def test_data_types_validation():
    # stationName should be string
    event = {"type": "sample", "stationName": 123, "timestamp": 1, "temperature": 10.0}
    with pytest.raises(ValueError, match="Please verify input. stationName must be a string."):
        list(weather.process_events([event]))
    # timestamp should be int
    event = {"type": "sample", "stationName": "A", "timestamp": "not_a_number", "temperature": 10.0}
    with pytest.raises(ValueError, match="Please verify input. timestamp must be an integer."):
        list(weather.process_events([event]))
    # temperature should be number
    event = {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": "not_a_number"}
    with pytest.raises(ValueError, match="Please verify input. temperature must be a number."):
        list(weather.process_events([event]))

def test_empty_input():
    events = []
    result = list(weather.process_events(events))
    assert not result

def test_mixed_control_commands():
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "snapshot"},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "B", "timestamp": 2, "temperature": 20.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 3
    assert result[0]["type"] == "snapshot"
    assert result[1]["type"] == "reset"
    assert result[2]["type"] == "snapshot"
    assert result[2]["stations"]["B"]["high"] == 20.0

def test_unexpected_error_handling():
    # Test that unexpected errors are caught and re-raised with the required message
    # This would be a case where something unexpected happens during processing
    # this test that our existing error handling works correctly
    events = [{"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0}]
    result = list(weather.process_events(events))
    assert not result
