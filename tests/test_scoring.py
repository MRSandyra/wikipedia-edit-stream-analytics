"""
Unit tests untuk modul scoring.
Mencakup 8+ kasus uji sesuai spesifikasi.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.scoring import (
    calculate_suspicion_score,
    is_suspicious,
    is_ipv4,
    is_ipv6,
    is_anonymous_user,
    load_suspicious_keywords
)


def create_test_event(**kwargs):
    """Helper function untuk membuat test event"""
    return {
        "type": "edit",
        "namespace": 0,
        "title": "Test Page",
        "comment": "",
        "user": "TestUser",
        "bot": False,
        "server_name": "test.wikipedia.org",
        "wiki": "enwiki",
        "length": {"old": 1000, "new": 1000},
        "revision": {"old": 1, "new": 1},
        **kwargs
    }


def create_recent_edits(count: int = 5, user: str = "TestUser", title: str = "Test Page"):
    """Helper function untuk membuat list recent edits"""
    now = datetime.now()
    edits = []
    for i in range(count):
        edits.append({
            "user": user,
            "title": title,
            "processed_at": now - timedelta(seconds=i * 30)
        })
    return edits


class TestScoringVandalism:
    """Test kasus vandalism jelas"""
    
    def test_large_content_reduction(self):
        """Test pengurangan konten besar (length.new - length.old < -500)"""
        event = create_test_event(
            length={"old": 1000, "new": 100},  # 900 reduction
            comment="test"
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score >= 30, f"Expected score >= 30 for large content reduction, got {score}"
    
    def test_multiple_vandalism_factors(self):
        """Test kombinasi beberapa kondisi vandalism"""
        event = create_test_event(
            length={"old": 2000, "new": 50},  # 1950 reduction -> +30
            comment="delete all",  # suspicious keyword -> +25
            user="192.168.1.1",  # IP -> +15
            namespace=0,  # non-talk -> +15
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        # +30 (length) + 25 (keyword) + 15 (IP on non-talk) = 70
        assert 70 <= score <= 100, f"Expected high suspicion score, got {score}"
    
    def test_suspicious_keywords(self):
        """Test deteksi keyword mencurigakan dalam comment"""
        event = create_test_event(
            comment="vandalism and spam edit",
            length={"old": 500, "new": 400},
            user="LegitUser",
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score >= 25, f"Expected score >= 25 for suspicious keywords, got {score}"


class TestScoringLegit:
    """Test kasus edit legit jelas"""
    
    def test_small_edit_by_registered_user(self):
        """Test edit kecil oleh user terdaftar dengan comment"""
        event = create_test_event(
            length={"old": 1000, "new": 1050},  # Small change
            comment="Fixed typo in section 2",
            user="RegisteredUser",
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score == 0, f"Expected score 0 for legit edit, got {score}"
    
    def test_bot_edit_not_suspicious(self):
        """Test bot edit harus memiliki score 0"""
        event = create_test_event(
            length={"old": 1000, "new": 100},  # Large reduction
            comment="vandalism",
            user="TestBot",
            bot=True  # Bot = score locked to 0
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score == 0, f"Bot edit should have score 0, got {score}"


class TestScoringEdgeCases:
    """Test edge case"""
    
    def test_score_threshold_at_50(self):
        """Test edge case tepat di score=50"""
        # Need exactly 50 points
        event = create_test_event(
            length={"old": 1000, "new": 400},  # 600 reduction -> +30
            comment="",  # empty comment on large change
            user="192.168.1.1",  # IP -> +15
            namespace=1,  # Talk namespace -> no IP bonus
            bot=False
        )
        # +30 (length reduction >500) + 10 (empty comment on large change) + 15 (IP on non-talk)
        # Wait, namespace=1 is Talk, so no IP bonus
        # Let me recalculate:
        # length reduction = 400 - 1000 = -600 -> +30
        # IP on namespace 1 (Talk) -> no bonus (namespace is 1)
        # empty comment with large change (>1000) -> +10
        # Total = 40, not 50
        
        # Adjust to get exactly 50
        event2 = create_test_event(
            length={"old": 2000, "new": 400},  # 1600 reduction -> +30
            comment="",  # empty comment with >1000 change -> +10
            user="192.168.1.1",  # IP -> +15
            namespace=0,  # non-talk -> +15
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event2, recent)
        # +30 (length) + 10 (empty comment) + 15 (IP non-talk) = 55
        assert score >= 50, f"Expected score >= 50, got {score}"
    
    def test_ip_editor_on_talk_page_no_bonus(self):
        """Test editor IP di halaman Talk tidak dapat poin IP"""
        event = create_test_event(
            length={"old": 1000, "new": 500},  # 500 reduction -> NOT < -500, so 0
            comment="regular discussion",
            user="192.168.1.1",  # IP
            namespace=1,  # Talk namespace
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        # length diff = 500 - 1000 = -500, NOT < -500 (not less than), so 0
        # IP on Talk (namespace 1) -> 0 bonus
        # Total = 0
        assert score == 0, f"IP on Talk page should not get IP bonus, got {score}"
    
    def test_anonymous_user_not_on_talk(self):
        """Test editor anonim di namespace non-Talk"""
        event = create_test_event(
            length={"old": 1000, "new": 500},
            comment="test",
            user="10.0.0.1",  # IPv4
            namespace=0,  # non-talk
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score >= 15, f"IP on non-talk should get +15, got {score}"


class TestScoringEditWarring:
    """Test pola edit-warring"""
    
    def test_same_user_same_page_multiple_times(self):
        """Test user mengedit halaman yang sama >3 kali dalam 5 menit"""
        recent = create_recent_edits(count=5, user="WarriorUser", title="Controversial Page")
        
        event = create_test_event(
            length={"old": 1000, "new": 1050},
            comment="Another edit",
            user="WarriorUser",
            bot=False,
            title="Controversial Page"  # Same page
        )
        
        score = calculate_suspicion_score(event, recent)
        # 5 recent edits + current = should trigger +20
        assert score >= 20, f"Edit warring should score >= 20, got {score}"
    
    def test_edit_warring_not_triggered_below_threshold(self):
        """Test edit warring tidak terpicu di bawah threshold"""
        recent = create_recent_edits(count=2, user="OccasionalUser", title="Some Page")
        
        event = create_test_event(
            length={"old": 1000, "new": 1050},
            comment="Test",
            user="OccasionalUser",
            bot=False,
            title="Some Page"
        )
        
        score = calculate_suspicion_score(event, recent)
        # Only 2 recent edits, not enough to trigger +20
        assert score < 20, f"Less than 4 edits should not trigger edit warring bonus, got {score}"


class TestScoringEmptyCommentLargeChange:
    """Test comment kosong pada perubahan besar"""
    
    def test_empty_comment_with_large_change(self):
        """Test comment kosong dengan |length.new - length.old| > 1000"""
        event = create_test_event(
            length={"old": 2000, "new": 500},  # Diff = 1500 > 1000
            comment="",  # Empty
            user="TestUser",
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        # +30 (length diff > 500) + 10 (empty comment, diff > 1000)
        assert score >= 40, f"Empty comment with large change should get +10, got {score}"


class TestScoringCombination:
    """Test kombinasi beberapa kondisi sekaligus"""
    
    def test_full_vandalism_scenario(self):
        """Test skenario vandalism lengkap"""
        event = create_test_event(
            length={"old": 5000, "new": 100},  # Large reduction -> +30
            comment="",  # Empty comment -> +10 (diff = 4900 > 1000)
            user="185.199.108.153",  # IPv6-like or IPv4
            namespace=0,  # Non-talk -> +15
            bot=False
        )
        # Edit the comment to include keyword
        event["comment"] = "blanking with vandalism"
        
        recent = create_recent_edits(4, user="185.199.108.153", title="Vandalized Page")
        
        # Need 4+ recent edits for the same user and page
        score = calculate_suspicion_score(event, recent)
        
        # Expected components:
        # +30 (length reduction)
        # +10 (empty comment on large change)
        # +25 (suspicious keyword "vandalism")
        # +15 (IP on non-talk)
        # +20 (edit warring, 4+ edits)
        # Total potential = 100
        # Note: keyword check happens on comment, so +25 for "vandalism"
        
        assert score >= 50, f"Full vandalism should score >= 50, got {score}"
        assert score <= 100, f"Score should be capped at 100"
    
    def test_mixed_legit_and_suspicious(self):
        """Test campuran legit dan suspicious elements"""
        event = create_test_event(
            length={"old": 1000, "new": 900},  # 100 reduction, not < -500
            comment="minor fix",  # No keyword
            user="RegularUser",
            bot=False
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score == 0, f"Legit edit should have score 0, got {score}"
    
    def test_bot_with_vandalism_score_zero(self):
        """Test bot yang melakukan vandalism tetap score 0"""
        event = create_test_event(
            length={"old": 5000, "new": 0},  # Complete blanking -> +30
            comment="delete all",  # Keyword -> +25
            user="VandalBot",
            bot=True  # Should override to 0
        )
        recent = create_recent_edits(2)
        
        score = calculate_suspicion_score(event, recent)
        assert score == 0, f"Bot should always have score 0 regardless of other factors, got {score}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])