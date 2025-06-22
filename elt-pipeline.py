from extract import *
from load import *
from utils.db_conn import *
import subprocess as sp
import luigi
import datetime
from typing import TypeVar, Callable


T = TypeVar("T")


class GlobalParams(luigi.Config):
    CurrentTimeStampParams = luigi.DateSecondParameter(default=datetime.datetime.now())


class dbtDebug(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> None:  # type: ignore
        pass

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"logs/dbt_debug/dbt_debug_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    "cd ./dbt_pactravel/ && dbt debug",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success Run dbt debug process")

                else:
                    print("Failed to run dbt debug process")

        except Exception as e:
            raise e


class dbtDeps(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> Callable:  # type: ignore
        return dbtDebug()  # type: ignore

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"logs/dbt_deps/dbt_deps_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    "cd ./dbt_pactravel/ && dbt deps",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success Run dbt deps process")

                else:
                    print("Failed to run dbt deps process")

        except Exception as e:
            raise e


class dbtRun(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> Callable:  # type: ignore
        return dbtDeps()  # type: ignore

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"logs/dbt_run/dbt_run_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    "cd ./dbt_pactravel/ && dbt run",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success running dbt data model")

                else:
                    print("Failed to run dbt data model")

        except Exception as e:
            raise e


class dbtTest(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> Callable:  # type: ignore
        return dbtRun()  # type: ignore

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"logs/dbt_test/dbt_test_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    "cd ./dbt_pactravel/ && dbt test",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success running testing and create a constraints")

                else:
                    print("Failed running testing and create constraints")

        except Exception as e:
            raise e


if __name__ == "__main__":
    extract()
    load()
    luigi.build([dbtDebug(), dbtDeps(), dbtRun(), dbtTest()], local_scheduler=True)