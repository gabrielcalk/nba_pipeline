from sqlalchemy import create_engine, Table, MetaData, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert
from pandas import DataFrame
from jinja2 import Environment
from logging import Logger

class PostgreSqlClient:
    def __init__(
        self,
        logger: Logger,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port
        self.logger = logger

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def execute_sql(self, sql: str) -> None:
        self.engine.execute(sql)
        
    def select(self, query: str) -> list[dict]:
        result = self.engine.execute(text(query))
        return [dict(row) for row in result]
    
    def select_max_id(self, table_name: str) -> int:
        if not self.table_exists(table_name):
            return 0
        result = self.select(f"select max(id) from {table_name}")
        return result[0]['max']
    
    def table_exists(self, table_name: str) -> bool:
        return inspect(self.engine).has_table(table_name)
    
    def create_table(self, table_name: str, table_file_name: str, tables_template: Environment) -> None:
        team_table = tables_template.get_template(f"{table_file_name}.sql.j2")
        exec_sql = team_table.render(table_name=table_name)
        self.execute_sql(exec_sql)
        self.logger.info(f"Table {table_name} created.")

    def insert(self, data: DataFrame, tables_template: Environment, table_name: str, file_name: str, chunk_size: int, mode: str) -> None:      
        if not self.table_exists(table_name):
            self.create_table(table_name, file_name, tables_template)
        
        table = Table(table_name, MetaData(), autoload_with=self.engine)
        
        for chunk in range(0, len(data), chunk_size):
            chunk_data = data.iloc[chunk:chunk + chunk_size]
            data_dict = chunk_data.to_dict(orient='records')
            stmt = insert(table).values(data_dict)
            self.engine.execute(stmt)
            self.logger.info(f"Inserted chunk {chunk//chunk_size + 1} into table {table_name}")

    def drop_tables(self, team_name: str, season: int) -> None:
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        team_tables = [table for table in tables if team_name in table.lower() and season in table.lower()]
        for table in team_tables:
            self.execute_sql(f"DROP TABLE {table} CASCADE")
            self.logger.info(f"Table {table} dropped.")

    def upsert(self, data: DataFrame, tables_template: Environment, table_name: str, file_name: str, chunk_size: int) -> None:
        if not self.table_exists(table_name):
            self.create_table(table_name, file_name, tables_template)

        table = Table(table_name, MetaData(), autoload_with=self.engine)
        
        for chunk in range(0, len(data), chunk_size):
            chunk_data = data.iloc[chunk:chunk + chunk_size]
            data_dict = chunk_data.to_dict(orient='records')

            stmt = insert(table).values(data_dict)
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={c.name: stmt.excluded[c.name] for c in table.columns if c.name != 'id'}
            )

            self.engine.execute(on_conflict_stmt)
            self.logger.info(f"Upserted chunk {chunk//chunk_size + 1} into table {table_name}")
