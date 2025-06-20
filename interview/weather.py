from typing import Any, Iterable, Generator


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    for event in events:
        if not isinstance(event, dict) or 'type' not in event:
            raise ValueError("Please verify input. Event must be a dictionary with 'type' field.")
        
        event_type = event.get('type')
        if event_type not in ['sample', 'control']:
            raise ValueError(f"Please verify input. Unknown message type: {event_type}")
        
        yield event
