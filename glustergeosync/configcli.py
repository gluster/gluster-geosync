import sys
from argparse import ArgumentParser

from glustergeosync.confutils import Config, ConfigSetException, \
    ConfigValueNotChangedException

def config_get(input_args):
    parser = ArgumentParser()
    parser.add_argument("--config-file", help="Config file", required=True)
    parser.add_argument("--name", help="Option name")

    args = parser.parse_args(input_args)

    conf = Config()
    conf.load_file(args.config_file)
    try:
        values = conf.get(args.name)
    except KeyError:
        print("Invalid config name: %s" % args.name)
        sys.exit(1)

    if values:
        print("%-25s  %s" % ("Name", "Value"))

    for key, value in values.items():
        print("%-25s  %s" % (key, value))


def config_help(input_args):
    parser = ArgumentParser()
    parser.add_argument("--name", help="Config name")
    args = parser.parse_args(input_args)

    cnf = Config()
    helpdata = cnf.gethelp(args.name)

    print("%-25s  %s" % ("Name", "Details"))
    for key, data in helpdata.items():
        detail = data["help"]
        detail += "(Default: %s" % repr(data.get("value", ""))
        choices = data.get("choices", None)
        if choices:
            detail += " Choices: %s" % repr(choices)

        detail += ")"

        print("%-25s  %s" % (key, detail))


def config_set(input_args):
    parser = ArgumentParser()
    parser.add_argument("--config-file", help="Config file", required=True)
    parser.add_argument("--name", help="Option name", required=True)
    parser.add_argument("--value", help="Option value", required=True)

    args = parser.parse_args(input_args)

    conf = Config()
    try:
        conf.setconfig(args.config_file, args.name, args.value)
        print("Config set successful")
    except ConfigSetException as err:
        key_valid, val_valid = err.args[0]
        if not key_valid:
            print("Invalid option name: %s" % args.name)

        if not val_valid:
            print("Invalid option value: %s" % args.value)

        if not key_valid or not val_valid:
            sys.exit(1)
    except ConfigValueNotChangedException:
        print("Config value not changed, same as existing value")
        sys.exit(2)
