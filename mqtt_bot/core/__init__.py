"""Core engine: digital twin, rules, state extraction, snapshot.

Class-agnostic. Adding a new device class is a config-only change
(``devices/<class>/class.json`` + a webxdc app); nothing in this
sub-package needs to know about specific device families.
"""
