from typing import Any, Iterable, Generator, Dict, Optional, Tuple


def validate_sample_event(event: dict[str, Any]) -> Tuple[str, int, float]:
    """Validate and extract sample event fields."""
    required_fields = ['stationName', 'timestamp', 'temperature']
    for key in required_fields:
        if key not in event:
            raise ValueError(
                "Please verify input. Sample must contain stationName, timestamp, and temperature."
            )

    station = event['stationName']
    temp = event['temperature']
    ts = event['timestamp']

    if not isinstance(station, str):
        raise ValueError("Please verify input. stationName must be a string.")
    if not isinstance(ts, int):
        raise ValueError("Please verify input. timestamp must be an integer.")
    if not isinstance(temp, (int, float)):
        raise ValueError("Please verify input. temperature must be a number.")

    return station, ts, float(temp)


def validate_control_event(event: dict[str, Any]) -> str:
    """Validate and extract control event command."""
    if 'command' not in event:
        raise ValueError(
            "Please verify input. Control message must contain 'command' field."
        )
    return event['command']


def update_station_data(
    stations_data: Dict[str, Dict[str, float]], station: str, temperature: float
) -> None:
    """Update high/low temperatures for a station."""
    if station not in stations_data:
        stations_data[station] = {'high': temperature, 'low': temperature}
    else:
        stations_data[station]['high'] = max(
            stations_data[station]['high'], temperature
        )
        stations_data[station]['low'] = min(
            stations_data[station]['low'], temperature
        )


def generate_snapshot_output(
    stations_data: Dict[str, Dict[str, float]], latest_timestamp: int
) -> dict[str, Any]:
    """Generate snapshot output."""
    return {
        'type': 'snapshot',
        'asOf': latest_timestamp,
        'stations': stations_data.copy()
    }


def generate_reset_output(latest_timestamp: int) -> dict[str, Any]:
    """Generate reset output."""
    return {
        'type': 'reset',
        'asOf': latest_timestamp
    }


def handle_sample_event(
    event: dict[str, Any],
    stations_data: Dict[str, Dict[str, float]],
    latest_timestamp: Optional[int]
) -> int:
    """Handle sample event and return updated latest timestamp."""
    station, ts, temp = validate_sample_event(event)
    if latest_timestamp is None or ts > latest_timestamp:
        latest_timestamp = ts
    update_station_data(stations_data, station, temp)
    return latest_timestamp


def handle_control_event(
    event: dict[str, Any],
    stations_data: Dict[str, Dict[str, float]],
    latest_timestamp: Optional[int]
) -> Tuple[Optional[dict[str, Any]], Optional[int]]:
    """Handle control event and return (output, new_latest_timestamp)."""
    command = validate_control_event(event)
    if command == 'snapshot':
        if latest_timestamp is not None:
            return generate_snapshot_output(stations_data, latest_timestamp), latest_timestamp
        return None, latest_timestamp
    if command == 'reset':
        if latest_timestamp is not None:
            return generate_reset_output(latest_timestamp), None
        return None, None
    raise ValueError(f"Please verify input. Unknown control command: {command}")


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    stations_data: Dict[str, Dict[str, float]] = {}
    latest_timestamp: Optional[int] = None

    for event in events:
        try:
            if not isinstance(event, dict) or 'type' not in event:
                raise ValueError(
                    "Please verify input. Event must be a dictionary with 'type' field."
                )

            event_type = event.get('type')
            if event_type not in ['sample', 'control']:
                raise ValueError(f"Please verify input. Unknown message type: {event_type}")

            if event_type == 'sample':
                latest_timestamp = handle_sample_event(event, stations_data, latest_timestamp)
            elif event_type == 'control':
                output, new_timestamp = handle_control_event(event, stations_data, latest_timestamp)
                if output is not None:
                    yield output
                if new_timestamp is None:  # Reset occurred
                    stations_data.clear()
                    latest_timestamp = None
                else:
                    latest_timestamp = new_timestamp

        except ValueError as e:
            raise ValueError(str(e)) from e
        except Exception as e:
            raise ValueError(f"Please verify input. Unexpected error: {str(e)}") from e

    yield from ()
