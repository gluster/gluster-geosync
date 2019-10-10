def validate_choices(default_data, value):
    return value in default_data["choices"]


DEFAULT_CONFIG = {
    "changelog-log-level": {
        "help": "Set Changelog log level",
        "value": "INFO",
        "choices": ["INFO", "TRACE"],
        "validation": validate_choices
    }
}
