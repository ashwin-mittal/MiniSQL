# TESTED

import sys

sys.path.append("src/")
from Query import Query


def main():
    if len(sys.argv) != 2:
        sys.exit("Usage: python main.py <query>")
    Query(sys.argv[1])


if __name__ == "__main__":
    main()
