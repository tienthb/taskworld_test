import os
import pandas as pd
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database


class PGController:
    def __init__(self, database):
        self.pg_user = os.getenv("POSTGRES_USER")
        self.pg_password = os.getenv("POSTGRES_PASSWORD")
        self.pg_host = os.getenv("POSTGRES_HOST")
        self.engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}@{self.pg_host}:5432/{database}")
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

    def _load_src_data(self, filepath):
        filename = os.path.basename(filepath)
        table_name = filename.replace(".csv", "")
        df = pd.read_csv(filepath)
        df.to_sql(
            table_name, 
            self.engine, 
            index=False,
            dtype={
                "active_date": sqlalchemy.types.Date,
                "user_id": sqlalchemy.types.VARCHAR(length=255),
                "workspace_id": sqlalchemy.types.VARCHAR(length=255),
                "total_activity": sqlalchemy.types.INTEGER(),
            },
            if_exists="replace"
        )
        print(f"{filepath} is loaded")

    def _calc_top_workspace(self):
        top_workspace_query = """
            WITH total_activities AS 
            (
                SELECT 
                    user_id, 
                    workspace_id, 
                    SUM(total_activity) AS total_activity
                FROM activity a
                GROUP BY user_id, workspace_id
            )
            ,rank_total_actitivities AS 
            (
                SELECT 
                    user_id,
                    workspace_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total_activity DESC) AS rn
                FROM total_activities
            )
            SELECT 
                user_id,
                workspace_id AS top_workspace
            FROM rank_total_actitivities
            WHERE rn = 1
        """

        df_workspace = pd.read_sql(top_workspace_query, self.engine)
        return df_workspace
    
    def _calc_longest_streak(self):
        longest_streak_query = """
            WITH de_dup AS 
            (
                SELECT user_id, active_date
                FROM activity a
                GROUP BY user_id, active_date
            )
            , row_num AS 
            (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY active_date) AS rn
                FROM de_dup
            )
            ,streak AS 
            (
                SELECT user_id, active_date - rn * INTERVAL '1 day', COUNT(1) AS streaks
                FROM row_num
                GROUP BY user_id, active_date - rn * INTERVAL '1 day'
            )
            SELECT user_id, MAX(streaks) AS longest_streak
            FROM streak
            GROUP BY user_id
        """
        df_streak = pd.read_sql(longest_streak_query, self.engine)
        return df_streak


    def calc_user_activity(self, source, table_name):
        self._load_src_data(source)
        df_workspace = self._calc_top_workspace()
        df_streak = self._calc_longest_streak()
        df_workspace = df_workspace.merge(df_streak, on="user_id")
        df_workspace.to_sql(table_name, self.engine, index=False, if_exists="replace")