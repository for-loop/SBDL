import argparse


class ArgParser:

    def __init__(self, description):
        parser = argparse.ArgumentParser(description=description)

        parser.add_argument(
            "env",
            type=str,
            nargs=1,
            metavar="environment",
            default=None,
            help="Environment should be 'local', 'qa', or 'prod'.",
        )

        parser.add_argument(
            "-d",
            "--date",
            type=str,
            nargs=1,
            metavar="load_date",
            default=["2022-08-02"],
            help="load date. The default is '2022-08-02'.",
        )

        args = parser.parse_args()

        if args.env:
            self.env = args.env[0].upper()
        if args.date:
            self.load_date = args.date[0]

    def get_all(self):
        return (self.env, self.load_date)
