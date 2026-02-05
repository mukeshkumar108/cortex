from typing import Optional, Dict, Any
import re


def get_time_of_day(hour: int) -> str:
    """Get time of day from hour"""
    if 5 <= hour < 12:
        return "morning"
    elif 12 <= hour < 17:
        return "afternoon"
    elif 17 <= hour < 21:
        return "evening"
    else:
        return "night"


def format_time_gap(minutes: int) -> str:
    """Format time gap into human-readable string"""
    if minutes < 1:
        return "just now"
    elif minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif minutes < 1440:  # Less than a day
        hours = minutes // 60
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    elif minutes < 2880:  # Less than 2 days
        return "yesterday"
    else:
        days = minutes // 1440
        return f"{days} day{'s' if days != 1 else ''} ago"


def extract_identity_updates(text: str) -> Dict[str, Any]:
    """Legacy helper (deprecated): local identity extraction is not used in Graphiti‑native v1."""
    updates = {}
    text_lower = text.lower()

    # Rejection patterns for non-names (states, adjectives, greetings, activities)
    name_rejects = {
        # States/conditions
        "tired", "exhausted", "sleepy", "awake", "busy", "free", "available",
        "working", "coding", "studying", "reading", "writing", "thinking",
        # Adjectives/emotions
        "fine", "good", "great", "okay", "ok", "bad", "sad", "happy", "excited",
        "angry", "frustrated", "worried", "nervous", "calm", "stressed",
        # Greetings/locations
        "here", "there", "back", "home", "away", "gone", "out", "in", "online", "offline",
        # Common filler words
        "sure", "yeah", "yes", "no", "not", "just", "really", "very"
    }

    # Extract name with validation
    name_patterns = [
        (r"my name is ([a-zA-Z]+)", 1),
        (r"i'?m ([a-zA-Z]+)", 1),
        (r"i am ([a-zA-Z]+)", 1),
        (r"call me ([a-zA-Z]+)", 1),
        (r"this is ([a-zA-Z]+)", 1),  # "Hi, this is Mukesh"
    ]

    for pattern, group_idx in name_patterns:
        match = re.search(pattern, text_lower)
        if match:
            candidate = match.group(group_idx).strip()
            candidate_lower = candidate.lower()

            # Reject if it's a non-name
            if candidate_lower not in name_rejects and len(candidate_lower) >= 2:
                updates['name'] = candidate.capitalize()
                updates['name_explicit'] = True
                break

    # Extract location/home
    location_patterns = [
        r"i live in ([^,.!?]+)",
        r"i'?m from ([^,.!?]+)",
        r"i am from ([^,.!?]+)",
        r"my home is ([^,.!?]+)",
        r"based in ([^,.!?]+)"
    ]
    for pattern in location_patterns:
        match = re.search(pattern, text_lower)
        if match:
            location = match.group(1).strip()
            # Only accept if it looks like a location (multiple words or capitalized)
            if len(location) >= 3:
                updates['home'] = location
                break

    # Extract timezone (if explicitly mentioned)
    timezone_pattern = r"timezone[:\s]+([A-Z]{3,4}|[A-Za-z]+/[A-Za-z_]+)"
    match = re.search(timezone_pattern, text)
    if match:
        updates['timezone'] = match.group(1)

    return updates


def _is_likely_name(word: str) -> bool:
    """Check if a word is likely a proper name"""
    # Common names that might appear lowercase
    common_names = {
        "john", "jane", "mike", "sarah", "david", "mary", "james", "emma",
        "mukesh", "ashley", "kaiser", "sophie", "alex", "sam", "chris", "jordan"
    }
    return word.lower() in common_names


def infer_mood(text: str) -> Optional[str]:
    """Infer mood from text using simple keyword matching"""
    text_lower = text.lower()

    # Mood keywords
    mood_map = {
        "excited": ["excited", "amazing", "awesome", "great", "wonderful", "fantastic"],
        "happy": ["happy", "glad", "pleased", "good", "nice", "yay"],
        "sad": ["sad", "unhappy", "down", "depressed", "blue"],
        "frustrated": ["frustrated", "annoyed", "irritated", "angry", "mad", "upset"],
        "anxious": ["anxious", "worried", "nervous", "stressed", "overwhelmed"],
        "tired": ["tired", "exhausted", "sleepy", "drained", "weary"],
        "calm": ["calm", "peaceful", "relaxed", "chill", "zen"],
        "confused": ["confused", "lost", "unclear", "puzzled"]
    }

    for mood, keywords in mood_map.items():
        for keyword in keywords:
            if keyword in text_lower:
                return mood

    return None


def extract_location(text: str) -> Optional[str]:
    """Extract current location from text"""
    text_lower = text.lower()

    location_patterns = [
        r"i'm at ([^,.!?]+)",
        r"i am at ([^,.!?]+)",
        r"at ([^,.!?]+)",
        r"in ([^,.!?]+)"
    ]

    # Common location indicators
    location_words = ["home", "work", "office", "gym", "park", "cafe", "restaurant", "store"]

    for pattern in location_patterns:
        match = re.search(pattern, text_lower)
        if match:
            location = match.group(1).strip()
            # Check if it's a meaningful location
            if any(word in location for word in location_words):
                return location

    return None


def extract_activity(text: str) -> Optional[str]:
    """Extract current activity from text"""
    text_lower = text.lower()

    activity_patterns = [
        r"i'm (\w+ing) ([^,.!?]+)",
        r"i am (\w+ing) ([^,.!?]+)",
        r"just (\w+ed) ([^,.!?]+)",
        r"about to (\w+) ([^,.!?]+)"
    ]

    # Common activities
    activities = [
        "working", "coding", "reading", "writing", "studying", "exercising",
        "walking", "running", "cooking", "eating", "sleeping", "relaxing",
        "watching", "listening", "playing", "meeting", "traveling"
    ]

    for pattern in activity_patterns:
        match = re.search(pattern, text_lower)
        if match:
            activity = match.group(0)
            return activity

    # Check for activity keywords
    for activity in activities:
        if activity in text_lower:
            return activity

    return None


def is_noise(text: str) -> bool:
    """Check if text is noise (too short or filler phrases)"""
    text = text.strip().lower()

    # Too short
    if len(text.split()) < 3:
        if extract_identity_updates(text):
            return False
        return True

    # Filler phrases
    noise_phrases = [
        "hey", "hi", "hello", "lol", "ok", "okay", "yeah", "yep", "nope",
        "are you there", "you there", "hello?", "hi there", "hey there"
    ]

    return text in noise_phrases


def detect_completion_patterns(text: str) -> bool:
    """Detect deterministic completion patterns"""
    text_lower = text.lower()

    completion_keywords = [
        "done", "finished", "completed", "accomplished", "achieved",
        "did it", "got it done", "wrapped up", "crossed off"
    ]

    # Check for past tense verbs indicating completion
    past_tense_patterns = [
        r"\b(went|did|made|completed|finished|got|achieved|accomplished)\b"
    ]

    for keyword in completion_keywords:
        if keyword in text_lower:
            return True

    for pattern in past_tense_patterns:
        if re.search(pattern, text_lower):
            return True

    return False


def detect_drop_patterns(text: str) -> bool:
    """Detect deterministic drop/cancel patterns"""
    text_lower = text.lower()

    drop_keywords = [
        "never mind", "nevermind", "ignore that", "ignore it", "scratch that",
        "forget it", "forget that", "drop it", "cancel it", "no longer",
        "not doing that", "don't do that", "do not do that"
    ]

    for keyword in drop_keywords:
        if keyword in text_lower:
            return True

    return False
