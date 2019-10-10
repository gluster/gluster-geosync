import sys


VALID_SUBCOMMANDS = ["worker"]


def main():
    args = sys.argv

    if len(args) < 2:
        print("Available sub commands: %s" % ", ".join(VALID_SUBCOMMANDS))
        sys.exit(1)

    subcmd = args[1]

    if subcmd == "worker":
        from glustergeosync import worker

        worker.start(args[2:])
    else:
        print("Unsupported sub command: %s. Available sub commands: %s" % (
            subcmd, ", ".join(VALID_SUBCOMMANDS)
        ))
        sys.exit(1)


if __name__ == "__main__":
    main()
