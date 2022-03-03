import sys

from paranames.util import read


def main():

    try:
        mode = sys.argv[1]
    except IndexError:
        mode = "csv"

    with sys.stdin as fin, sys.stdout as fout:
        fout.write(read(fin, io_format=mode).to_latex(index=False))


if __name__ == "__main__":
    main()
