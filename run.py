import argparse
import json
import logging
import os

import pandas as pd

from database import WCSTNormalizeDatabase

ID_KEY = "Number"

PRETEST_AGE_KEY = "Age_PreTest"
POSTTEST_AGE_KEY = "Age_PostTest"

POSTTEST_SUFFIX = "_2"

EDUCATION_KEY = "Education"

TARGET_KEY = [
    "Total Error",
    "Percent Error",
    "Perseverative Responses",
    "Perseverative Errors",
    "Percent Perseverative Errors",
    "Nonperseverative Errors",
    "Percent Nonperseverative Errors",
]


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("norm_dir", type=str)
    parser.add_argument("raw_data", type=str)

    parser.add_argument("-o", "--output-path", type=str,
                        default="wcstst_std.csv")

    args = parser.parse_args()
    return args


def main(args):
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    db = WCSTNormalizeDatabase.build_from_dir(args.norm_dir)

    raw_data = pd.read_excel(args.raw_data)

    all_keys = (
        [ID_KEY, PRETEST_AGE_KEY, EDUCATION_KEY]
        + TARGET_KEY
        + [POSTTEST_AGE_KEY]
        + [target + POSTTEST_SUFFIX for target in TARGET_KEY]
    )
    std_data = dict(
        (key, []) for key in all_keys
    )

    for idx, row in raw_data.iterrows():
        logging.info(
            f"[ {idx + 1} / {len(raw_data)} ] Processing Row {idx + 1}...")

        row = row.to_dict()
        id = row[ID_KEY]
        pretest_age = row[PRETEST_AGE_KEY]
        posttest_age = row[POSTTEST_AGE_KEY]
        education = row[EDUCATION_KEY]

        std_data[ID_KEY].append(id)
        std_data[PRETEST_AGE_KEY].append(pretest_age)
        std_data[POSTTEST_AGE_KEY].append(posttest_age)
        std_data[EDUCATION_KEY].append(education)

        for target in TARGET_KEY:
            pretest_value = row[target]
            posttest_value = row[target + POSTTEST_SUFFIX]
            std_data[target].append(
                db.retrieval(pretest_age, education, target, pretest_value)
            )
            std_data[target + POSTTEST_SUFFIX].append(
                db.retrieval(pretest_age, education, target, posttest_value)
            )

    std_data = pd.DataFrame.from_dict(std_data)
    std_data.to_csv(args.output_path, index=False)


if __name__ == "__main__":
    args = parse_args()
    main(args)
