#!/usr/bin/env python

import numpy as np
import click


@click.command()
@click.option("--train-frac", default=0.8, type=float)
@click.option("--dev-frac", default=0.1, type=float)
@click.option("--test-frac", default=0.1, type=float)
@click.option("--num-samples", "-n", required=True, type=int)
@click.option("--with-header", is_flag=True)
def main(train_frac, dev_frac, test_frac, num_samples, with_header):
    splits = np.array(["train", "dev", "test"])
    fractions = np.array([train_frac, dev_frac, test_frac])
    if with_header:
        print("split")
    print("\n".join(np.random.choice(a=splits, p=fractions, size=num_samples)))


if __name__ == "__main__":
    main()
