import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy_utils import database_exists, create_database

from controller.pg_controller import PGController


POSTGRES_DB = "test_warehouse"

class TestETLPipeline(unittest.TestCase):
    def setUp(self) -> None:
        # create test db
        self.pg = PGController(POSTGRES_DB)
        if not database_exists(self.pg.engine.url):
            create_database(self.pg.engine.url)
        self.engine = self.pg.engine


    def tearDown(self) -> None:
        self.engine.dispose()


    def test_calc_user_activity(self):
        data = {
            "user_id": ["5bfd0e8d472bcf0009a1014d"],
            "top_workspace": ["63b46191c64d0b00068215e8"],
            "longest_streak": [5]
        }
        df_expected = pd.DataFrame(data)
        test_data_path = "/src/tests/activity.csv"
        self.pg.calc_user_activity(test_data_path, "user_activity")
        df_result = pd.read_sql("SELECT * FROM user_activity", self.engine)
        assert_frame_equal(df_result, df_expected)
        

        # print(self.pg_container.driver)
        # print(self.pg_container.driver)
    #     pg_user = 
    #     pg = PGController()
    