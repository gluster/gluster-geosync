import sys


VALID_SUBCOMMANDS = ["worker", "config-get", "config-help", "config-set"]


def main():
    args = sys.argv

    if len(args) < 2:
        print("Available sub commands: %s" % ", ".join(VALID_SUBCOMMANDS))
        sys.exit(1)

    subcmd = args[1]
    args = args[2:]

    if subcmd not in VALID_SUBCOMMANDS:
        print("Unsupported sub command: %s. Available sub commands: %s" % (
            subcmd, ", ".join(VALID_SUBCOMMANDS)
        ))
        sys.exit(1)

    if subcmd == "worker":
        from glustergeosync import worker

        worker.start(args)
    elif subcmd.startswith("config-"):
        from glustergeosync import configcli

        getattr(configcli, subcmd.replace("-", "_"))(args)


if __name__ == "__main__":
    main()
