import os
import json

from glustergeosync.conf import DEFAULT_CONFIG


class ConfigSetException(Exception):
    pass


class ConfigValueNotChangedException(Exception):
    pass


class Config:
    def __init__(self):
        self.newconf = {}
        self.data = {}

    def load_file(self, filepath):
        with open(filepath) as config_file:
            self.newconf = json.load(config_file)
        self._build()

    def load_dict(self, newconf):
        self.newconf = newconf
        self._build()

    def _build(self):
        # Build new dictionary by applying custom config on top of
        # default config, which will be used to provide config to caller
        for key in DEFAULT_CONFIG:
            self.data[key] = self.newconf.get(
                key,
                # Get default configuration value from the file
                DEFAULT_CONFIG.get(key)["value"]
            )

    def get(self, key):
        if key is None or key == "":
            return self.data

        return {key: self.data[key]}

    def gethelp(self, key=None):
        if key is None or key == "":
            return DEFAULT_CONFIG

        return {key: DEFAULT_CONFIG[key]}

    def validate(self, key, value):
        keydata = DEFAULT_CONFIG.get(key, None)

        # Invalid Key
        if keydata is None:
            return (False, False)

        validate_func = keydata.get("validation", None)

        # Any value is accepted, Do only type validation
        if validate_func is None:
            if type(value) != type(DEFAULT_CONFIG[key].get("value", "")):
                return (True, False)

            return (True, True)

        return (True, validate_func(DEFAULT_CONFIG[key], value))

    def setconfig(self, filepath, key, value):
        key_valid, value_valid = self.validate(key, value)
        if key_valid and value_valid:
            with open(filepath) as conffile:
                data = json.load(conffile)
                if data[key] == value:
                    raise ConfigValueNotChangedException()

                data[key] = value

            with open(filepath + ".tmp", "w") as conffile:
                conffile.write(json.dumps(data, indent=4))

            os.rename(filepath + ".tmp", filepath)
            self.data[key] = value
        else:
            raise ConfigSetException((key_valid, value_valid))
