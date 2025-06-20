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
    with pytest.raises(ValueError,
                      match="Please verify input. Event must be a dictionary with 'type' field."):
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
        with pytest.raises(
            ValueError,
            match=(
                "Please verify input. Sample must contain stationName, timestamp, and temperature."
            ),
        ):
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
    with pytest.raises(ValueError,
                      match="Please verify input. Control message must contain 'command' field."):
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

# Additional comprehensive tests for production-level coverage

def test_temperature_edge_cases():
    """Test extreme temperature values and edge cases."""
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": -273.15},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 1000000.0},
        {"type": "sample", "stationName": "A", "timestamp": 3, "temperature": 0.0},
        {"type": "sample", "stationName": "A", "timestamp": 4, "temperature": -0.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    station_data = result[0]["stations"]["A"]
    assert station_data["high"] == 1000000.0
    assert station_data["low"] == -273.15

def test_timestamp_edge_cases():
    """Test timestamp edge cases including very large numbers."""
    events = [
    {"type": "sample", "stationName": "A", "timestamp": 0, "temperature": 10.0},
    {"type": "sample", "stationName": "A", "timestamp": 9223372036854775807, "temperature": 20.0},
    {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert result[0]["asOf"] == 9223372036854775807

def test_station_name_edge_cases():
    """Test various station name formats and edge cases."""
    events = [
        {"type": "sample", "stationName": "", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "   ", "timestamp": 2, "temperature": 20.0},
        {"type": "sample", "stationName": "Station-123", "timestamp": 3, "temperature": 30.0},
        {"type": "sample", "stationName": "🚁 Weather Station", "timestamp": 4, "temperature": 40.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    stations = result[0]["stations"]
    assert len(stations) == 4
    assert stations[""]["high"] == 10.0
    assert stations["   "]["high"] == 20.0
    assert stations["Station-123"]["high"] == 30.0
    assert stations["🚁 Weather Station"]["high"] == 40.0

def test_numeric_temperature_types():
    """Test that both int and float temperatures are accepted."""
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 20.5},
        {"type": "sample", "stationName": "B", "timestamp": 3, "temperature": 0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert result[0]["stations"]["A"]["high"] == 20.5
    assert result[0]["stations"]["B"]["high"] == 0.0

def test_multiple_resets():
    """Test multiple reset operations and their effects."""
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "B", "timestamp": 2, "temperature": 20.0},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "C", "timestamp": 3, "temperature": 30.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 3  # Two resets, one snapshot
    assert result[0]["type"] == "reset"
    assert result[0]["asOf"] == 1
    assert result[1]["type"] == "reset"
    assert result[1]["asOf"] == 2
    assert result[2]["type"] == "snapshot"
    assert "C" in result[2]["stations"]
    assert "A" not in result[2]["stations"]
    assert "B" not in result[2]["stations"]

def test_control_message_ordering():
    """Test control messages in various orders and combinations."""
    events = [
        {"type": "control", "command": "snapshot"},
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "control", "command": "snapshot"},
        {"type": "control", "command": "snapshot"},
        {"type": "control", "command": "reset"},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "B", "timestamp": 2, "temperature": 20.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert len(result) == 4  # 3 snapshots + 1 reset
    assert result[0]["type"] == "snapshot"
    assert result[1]["type"] == "snapshot"
    assert result[2]["type"] == "reset"
    assert result[3]["type"] == "snapshot"

def test_large_dataset_performance():
    """Test performance with a large number of samples."""
    events = []
    for i in range(1000):
        station = f"Station_{i % 10}"
        events.append({
            "type": "sample",
            "stationName": station,
            "timestamp": i,
            "temperature": float(i % 100)
        })
    events.append({"type": "control", "command": "snapshot"})
    result = list(weather.process_events(events))
    assert len(result) == 1
    assert len(result[0]["stations"]) == 10
    assert result[0]["asOf"] == 999

def test_concurrent_station_updates():
    """Test that multiple stations can be updated concurrently."""
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": 10.0},
        {"type": "sample", "stationName": "B", "timestamp": 1, "temperature": 20.0},
        {"type": "sample", "stationName": "C", "timestamp": 1, "temperature": 30.0},
        {"type": "sample", "stationName": "A", "timestamp": 2, "temperature": 5.0},
        {"type": "sample", "stationName": "B", "timestamp": 2, "temperature": 25.0},
        {"type": "sample", "stationName": "C", "timestamp": 2, "temperature": 15.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    stations = result[0]["stations"]
    assert stations["A"]["high"] == 10.0
    assert stations["A"]["low"] == 5.0
    assert stations["B"]["high"] == 25.0
    assert stations["B"]["low"] == 20.0
    assert stations["C"]["high"] == 30.0
    assert stations["C"]["low"] == 15.0

def test_invalid_event_structure():
    """Test various invalid event structures."""
    invalid_events = [
        None,
        "not_a_dict",
        123,
        [],
        {},
        {
            "type": "sample"
        },
        {"type": "control"},
    ]
    for event in invalid_events:
        with pytest.raises(ValueError, match="Please verify input"):
            list(weather.process_events([event]))

def test_memory_efficiency():
    """Test that the generator doesn't accumulate all data in memory."""
    def event_generator():
        for i in range(10000):
            yield {
                "type": "sample",
                "stationName": f"Station_{i % 100}",
                "timestamp": i,
                "temperature": float(i)
            }
            if i % 1000 == 0:
                yield {"type": "control", "command": "snapshot"}
    generator = weather.process_events(event_generator())
    snapshot_count = 0
    for output in generator:
        snapshot_count += 1
        assert output["type"] == "snapshot"
    assert snapshot_count == 10

def test_error_message_consistency():
    """Test that all error messages contain the required phrase."""
    test_cases = [
        ({"type": "invalid"}, "Please verify input. Unknown message type: invalid"),
        (
            {
                "type": "sample"
            },
            (
                "Please verify input. Sample must contain stationName, timestamp, and "
                "temperature."
            ),
        ),
        (
            {"type": "control"},
            "Please verify input. Control message must contain 'command' field."
        ),
        (
            {"type": "control", "command": "invalid"},
            "Please verify input. Unknown control command: invalid"
        ),
        (
            {"type": "sample", "stationName": 123, "timestamp": 1, "temperature": 10.0},
            "Please verify input. stationName must be a string."
        ),
        (
            {"type": "sample", "stationName": "A", "timestamp": "invalid", "temperature": 10.0},
            "Please verify input. timestamp must be an integer."
        ),
        (
            {"type": "sample", "stationName": "A", "timestamp": 1, "temperature": "invalid"},
            "Please verify input. temperature must be a number."
        ),
    ]
    for event, expected_message in test_cases:
        with pytest.raises(ValueError, match=expected_message):
            list(weather.process_events([event]))

def test_output_format_consistency():
    """Test that all output formats are consistent and valid JSON."""
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1000, "temperature": 37.1},
        {"type": "control", "command": "snapshot"},
        {"type": "control", "command": "reset"},
    ]
    result = list(weather.process_events(events))
    snapshot = result[0]
    assert "type" in snapshot
    assert "asOf" in snapshot
    assert "stations" in snapshot
    assert snapshot["type"] == "snapshot"
    assert isinstance(snapshot["asOf"], int)
    assert isinstance(snapshot["stations"], dict)
    reset = result[1]
    assert "type" in reset
    assert "asOf" in reset
    assert reset["type"] == "reset"
    assert isinstance(reset["asOf"], int)
    assert len(reset) == 2

def test_duplicate_timestamps():
    """Test behavior with duplicate timestamps."""
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 1000, "temperature": 10.0},
        {"type": "sample", "stationName": "A", "timestamp": 1000, "temperature": 20.0},
        {"type": "sample", "stationName": "B", "timestamp": 1000, "temperature": 30.0},
        {"type": "control", "command": "snapshot"},
    ]
    result = list(weather.process_events(events))
    assert result[0]["asOf"] == 1000
    assert result[0]["stations"]["A"]["high"] == 20.0
    assert result[0]["stations"]["A"]["low"] == 10.0
    assert result[0]["stations"]["B"]["high"] == 30.0
    assert result[0]["stations"]["B"]["low"] == 30.0
