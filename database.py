import logging
import math
import os
from typing import Union

import pandas as pd


class WCSTNormalizeDatabase:

    def __init__(self, db: pd.DataFrame):
        self.db = db

    @classmethod
    def build_from_dir(cls, data_dir: str):
        db_dict = {
            "min_age": [],
            "max_age": [],
            "min_education": [],
            "max_education": [],
            "normative_path": [],
        }

        def _parse_fname(fname: str):
            basename = fname.split(".")[0]
            min_age, max_age, min_edu, max_edu = basename.split("_")
            min_age = int(min_age) // 10000
            max_age = math.ceil(int(max_age) / 10000)

            min_edu = int(min_edu)
            max_edu = int(max_edu)

            return min_age, max_age, min_edu, max_edu

        for fname in os.listdir(data_dir):
            min_age, max_age, min_edu, max_edu = _parse_fname(fname)

            db_dict["min_age"].append(min_age)
            db_dict["max_age"].append(max_age)
            db_dict["min_education"].append(min_edu)
            db_dict["max_education"].append(max_edu)

            normative_path = os.path.abspath(os.path.join(data_dir, fname))
            db_dict["normative_path"].append(normative_path)

        db = pd.DataFrame.from_dict(db_dict)
        db = db.sort_values(by=["min_age", "min_education"])
        return cls(db)

    def export_db(self, output_path):
        self.db.to_csv(output_path, index=False)

    @staticmethod
    def _map_to_std_score(
        sheet_path: str,
        attribute_name: str,
        value: Union[int, float],
        percentage: bool = False,
    ):
        try:
            df = pd.read_excel(sheet_path, sheet_name=attribute_name)
            raw_score = df.iloc[:, -1]
            std_score = df.iloc[:, -2]

            mapping = {raw: std for raw, std in zip(raw_score, std_score)}

            if percentage:
                value = math.ceil(value * 100)

            min_raw = min(raw_score)
            max_raw = max(raw_score)

            if value < min_raw:
                return 145.5
            if value > max_raw:
                return 55

            return mapping[value]
        except Exception as e:
            return -1

    def retrieval(self, age: float, education: int, attribute_name: str, value):
        age = math.ceil(age)
        if education == 12:
            row = self.db[
                (age >= self.db["min_age"])
                & (age < self.db["max_age"])
                & (education == self.db["min_education"])
            ]
        else:
            row = self.db[
                (age >= self.db["min_age"])
                & (age < self.db["max_age"])
                & (education >= self.db["min_education"])
                & (education <= self.db["max_education"])
            ]

        if row.empty:
            logging.info("No any matches from database, skip")
            return -1

        normative_path = row["normative_path"].values[0]

        attribute_name = attribute_name.replace("_2", "")
        if attribute_name in [
            "Percent Error",
            "Percent Perseverative Responses",
            "Percent Perseverative Errors",
            "Percent Nonperseverative Errors",
        ]:
            std_score = self._map_to_std_score(
                normative_path, attribute_name, value, percentage=True
            )
        else:
            std_score = self._map_to_std_score(
                normative_path, attribute_name, value, percentage=False
            )

        return std_score


if __name__ == "__main__":
    db = WCSTNormalizeDatabase.build_from_dir(
        "/home/navi/codespace/wcst_data_process/data/WCST_Normative"
    )

    score = db.retrieval(
        age=66.03, education=14, attribute_name="Total Error", value=38
    )
    print(score)
    score = db.retrieval(age=123, education=14,
                         attribute_name="Total Error", value=38)
    print(score)
    score = db.retrieval(
        age=69.03, education=12, attribute_name="Total Error", value=38
    )
    print(score)
