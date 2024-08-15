from lib.ArgParser import ArgParser
from EtlProcessor import EtlProcessor


def main():
    args = ArgParser("SBDL")
    ep = EtlProcessor(args)
    ep.process()


if __name__ == "__main__":
    main()
