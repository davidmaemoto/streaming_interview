from typing import Any, Iterable, Generator, Dict, Optional


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    stations_data: Dict[str, Dict[str, float]] = {}
    latest_timestamp: Optional[int] = None

    for event in events:
        if not isinstance(event, dict) or 'type' not in event:
            raise ValueError("Please verify input. Event must be a dictionary with 'type' field.")
        event_type = event.get('type')
        if event_type not in ['sample', 'control']:
            raise ValueError(f"Please verify input. Unknown message type: {event_type}")

        if event_type == 'sample':
            for key in ['stationName', 'timestamp', 'temperature']:
                if key not in event:
                    raise ValueError("Please verify input. Sample must contain stationName, timestamp, and temperature.")
            station = event['stationName']
            temp = event['temperature']
            ts = event['timestamp']
            if latest_timestamp is None or ts > latest_timestamp:
                latest_timestamp = ts
            if station not in stations_data:
                stations_data[station] = {'high': temp, 'low': temp}
            else:
                stations_data[station]['high'] = max(stations_data[station]['high'], temp)
                stations_data[station]['low'] = min(stations_data[station]['low'], temp)
            continue
        if event_type == 'control':
            if event.get('command') == 'snapshot':
                if latest_timestamp is not None:
                    yield {
                        'type': 'snapshot',
                        'asOf': latest_timestamp,
                        'stations': stations_data.copy()
                    }
            elif event.get('command') == 'reset':
                if latest_timestamp is not None:
                    yield {
                        'type': 'reset',
                        'asOf': latest_timestamp
                    }
                stations_data.clear()
                latest_timestamp = None
            continue
    yield from ()
