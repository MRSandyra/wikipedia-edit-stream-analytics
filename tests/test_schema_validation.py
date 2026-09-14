"""
Unit tests untuk validasi schema Pydantic.
Memastikan event malformed/field hilang ditolak.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.schema import EventData, EventMeta, WikimediaEvent


def create_valid_event(**overrides):
    """Helper untuk membuat valid event"""
    default_event = {
        "type": "message",
        "topic": "/sigma/mainstream/event/mediawiki.recentchange",
        "source": "unicorn",
        "specversion": "1.0",
        "id": "test-event-123",
        "time": datetime.now().isoformat(),
        "data": {
            "$schema": "https://wikitech.wikimedia.org/schemas/mediawiki/recentchange/1.0.0",
            "id": "12345",
            "type": "edit",
            "namespace": 0,
            "title": "Test Page",
            "comment": "Test edit",
            "timestamp": int(datetime.now().timestamp()),
            "user": "TestUser",
            "bot": False,
            "server_url": "https://test.wikipedia.org",
            "server_name": "test.wikipedia.org",
            "server_script_path": "/w",
            "wiki": "testwiki",
            "parsedcomment": "Test edit",
            "minor": False,
            "length": {"old": 1000, "new": 1050},
            "revision": {"old": 123, "new": 124},
            "namespacename": "Mainspace",
            "pageurl": "https://test.wikipedia.org/wiki/Test_Page",
            "diffurl": "https://test.wikipedia.org/w/index.php?diff=124&oldid=123"
        },
        "meta": {
            "id": "event-123",
            "dt": datetime.now().isoformat(),
            "topic": "/sigma/mainstream/event/mediawiki.recentchange",
            "partition": 0,
            "offset": 1000
        }
    }
    default_event.update(overrides)
    return default_event


class TestValidEvents:
    """Test bahwa event valid diterima"""
    
    def test_valid_event_accepted(self):
        """Test event valid diterima oleh schema"""
        event_data = create_valid_event()
        try:
            # Convert time fields to proper datetime
            event_data["time"] = datetime.now()
            event_data["data"]["timestamp"] = int(datetime.now().timestamp())
            
            event = WikimediaEvent(**event_data)
            assert event.id == "test-event-123"
            assert event.type == "message"
        except ValidationError as e:
            pytest.fail(f"Valid event should be accepted: {e}")
    
    def test_valid_event_minimal_fields(self):
        """Test event valid dengan field minimal"""
        event_data = create_valid_event()
        event_data["time"] = datetime.now()
        event_data["data"]["timestamp"] = int(datetime.now().timestamp())
        
        # Remove optional fields
        event_data["data"].pop("namespacename", None)
        event_data["data"].pop("pageurl", None)
        event_data["data"].pop("diffurl", None)
        event_data["data"].pop("log_type", None)
        event_data["data"].pop("log_action", None)
        event_data["data"].pop("log_params", None)
        
        try:
            event = WikimediaEvent(**event_data)
            assert event.data.namespacename is None
            assert event.data.pageurl is None
        except ValidationError as e:
            pytest.fail(f"Event with minimal fields should be accepted: {e}")


class TestMalformedEvents:
    """Test bahwa event malformed ditolak"""
    
    def test_missing_required_field(self):
        """Test event dengan field required yang hilang ditolak"""
        event_data = create_valid_event()
        event_data["time"] = datetime.now()
        del event_data["type"]  # Required field missing
        
        with pytest.raises(ValidationError) as exc_info:
            WikimediaEvent(**event_data)
        
        assert "type" in str(exc_info.value)
    
    def test_invalid_type(self):
        """Test event dengan tipe data salah ditolak"""
        event_data = create_valid_event()
        event_data["time"] = datetime.now()
        event_data["data"]["bot"] = "not-a-boolean"  # Should be bool
        
        with pytest.raises(ValidationError):
            EventData(**event_data["data"])
    
    def test_missing_nested_field(self):
        """Test event dengan nested field yang hilang ditolak"""
        event_data = create_valid_event()
        event_data["time"] = datetime.now()
        del event_data["meta"]["partition"]  # Required nested field
        
        with pytest.raises(ValidationError) as exc_info:
            WikimediaEvent(**event_data)
        
        assert "partition" in str(exc_info.value)
    
    def test_empty_dict_as_event(self):
        """Test event kosong ditolak"""
        with pytest.raises(ValidationError):
            WikimediaEvent()
    
    def test_none_values(self):
        """Test event dengan None values ditolak untuk field wajib"""
        event_data = create_valid_event()
        event_data["time"] = datetime.now()
        event_data["data"]["title"] = None  # Required field set to None
        
        with pytest.raises(ValidationError):
            EventData(**event_data["data"])
    
    def test_invalid_namespace_type(self):
        """Test event dengan namespace bukan integer ditolak"""
        event_data = create_valid_event()
        event_data["data"]["namespace"] = "not-an-integer"
        
        with pytest.raises(ValidationError):
            EventData(**event_data["data"])
    
    def test_invalid_timestamp(self):
        """Test event dengan timestamp invalid ditolak"""
        event_data = create_valid_event()
        event_data["time"] = "not-a-datetime"
        
        with pytest.raises(ValidationError):
            WikimediaEvent(**event_data)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])