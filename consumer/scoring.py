"""
Scoring module untuk menghitung suspicion score dari edit Wikipedia.
Implementasi aturan rule-based sesuai spesifikasi.
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

import yaml


# Default suspicious keywords jika file tidak ditemukan
DEFAULT_KEYWORDS = [
    "vandalism", "test edit", "123", "asdf", "qwerty", "blanking",
    "spam", "delete all", "remove all", "nonsense", "testing",
    "hello world", "crap", "stupid", "idiot", "bullshit", "nsfw",
    "porn", "sex", "xxx", "penis", "vagina", "hitler", "nazi",
    "kill", "die", "death", "suicide", "hack", "hacked", "deface"
]


def load_suspicious_keywords(config_path: str = "../config/suspicious_keywords.yaml") -> List[str]:
    """Load suspicious keywords dari YAML config file"""
    try:
        config_file = Path(__file__).parent / config_path
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return config.get("suspicious_keywords", DEFAULT_KEYWORDS)
    except Exception as e:
        print(f"Warning: Failed to load suspicious keywords: {e}")
    
    return DEFAULT_KEYWORDS


def is_ipv4(text: str) -> bool:
    """Check if text is a valid IPv4 address"""
    pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
    if re.match(pattern, text):
        # Validate each octet is 0-255
        parts = text.split(".")
        return all(0 <= int(p) <= 255 for p in parts)
    return False


def is_ipv6(text: str) -> bool:
    """Check if text looks like an IPv6 address"""
    # Simplified IPv6 pattern - matches most common formats
    pattern = r"^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$|^::1$|^(::ffff:)?\d+\.\d+\.\d+\.\d+$"
    return bool(re.match(pattern, text))


def is_anonymous_user(username: str) -> bool:
    """Check if username is an IP address (anonymous editor)"""
    if not username:
        return False
    return is_ipv4(username) or is_ipv6(username)


def calculate_suspicion_score(event_data: Dict[str, Any], 
                              recent_edits: List[Dict[str, Any]],
                              suspicious_keywords: List[str] = None) -> int:
    """
    Calculate suspicion score (0-100) berdasarkan aturan rule-based.
    
    Aturan:
    - Pengurangan konten besar: length.new - length.old < -500 -> +30
    - Editor anonim (IPv4/IPv6) edit namespace selain Talk -> +15
    - Comment kosong pada perubahan besar (>1000) -> +10
    - Comment mengandung suspicious keyword -> +25
    - User sama edit halaman sama >3 kali dalam 5 menit -> +20
    - bot == true -> score dikunci ke 0
    
    Returns: suspicion score (0-100)
    """
    if suspicious_keywords is None:
        suspicious_keywords = load_suspicious_keywords()
    
    score = 0
    
    # Check if bot - if so, score is locked to 0
    if event_data.get("bot", False):
        return 0
    
    # Rule 1: Pengurangan konten besar
    length_old = event_data.get("length", {}).get("old", 0)
    length_new = event_data.get("length", {}).get("new", 0)
    
    if length_old and length_new:
        length_diff = length_new - length_old
        if length_diff < -500:
            score += 30
    
    # Rule 2: Editor anonim di namespace selain Talk
    namespace = event_data.get("namespace", 0)
    user = event_data.get("user", "")
    
    # Namespace 1 = Talk, 0 = Main/Article
    if is_anonymous_user(user) and namespace != 1:
        score += 15
    
    # Rule 3: Comment kosong pada perubahan besar
    comment = event_data.get("comment", "")
    length_abs_diff = abs(length_new - length_old)
    
    if not comment.strip() and length_abs_diff > 1000:
        score += 10
    
    # Rule 4: Suspicious keywords in comment
    comment_lower = comment.lower()
    for keyword in suspicious_keywords:
        if keyword.lower() in comment_lower:
            score += 25
            break  # Only add once even if multiple keywords match
    
    # Rule 5: User sama edit halaman sama >3 kali dalam 5 menit
    user_edits = []
    current_time = datetime.now()
    window_start = current_time - timedelta(minutes=5)
    
    for edit in recent_edits:
        edit_time = edit.get("processed_at", current_time)
        if isinstance(edit_time, str):
            try:
                edit_time = datetime.fromisoformat(edit_time.replace("Z", "+00:00"))
            except:
                edit_time = current_time
        
        if edit_time >= window_start:
            if edit.get("user") == user and edit.get("title") == event_data.get("title"):
                user_edits.append(edit)
    
    # >3 kali berarti 4 atau lebih
    if len(user_edits) >= 4:
        score += 20
    
    # Cap score at 100
    return min(score, 100)


def is_suspicious(score: int, threshold: int = 50) -> bool:
    """Check if edit is suspicious based on score threshold"""
    return score >= threshold