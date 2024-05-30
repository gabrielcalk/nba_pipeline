from sqlalchemy import create_engine, Table, MetaData, inspect
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert 
from pandas import DataFrame
from jinja2 import Environment

class PostgreSqlClient:
    """
    A client for querying postgresql database.
    """

    def __init__(
        self,
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

    def select_all(self, table: Table) -> list[dict]:
        """
        Execute SQL code provided and returns the result in a list of dictionaries.
        This method should only be used if you expect a resultset to be returned.
        """
        return [dict(row) for row in self.engine.execute(table.select()).all()]
    
    def table_exists(self, table_name: str) -> bool:
        """
        Checks if the table already exists in the database.
        """
        return inspect(self.engine).has_table(table_name)
    
    def create_table(self, table_name: str, tables_template: Environment) -> None:
        team_table = tables_template.get_template(f"{table_name}.sql")
        exec_sql = team_table.render()
        self.execute_sql(exec_sql)

    def insert(self, data: DataFrame, tables_template: Environment, table_name: str) -> None:
        if not self.table_exists(table_name):
            self.create_table(table_name, tables_template)
        
        data_dict = data.to_dict(orient='records')
        table = Table(table_name, MetaData(), autoload_with=self.engine)
        stmt = insert(table).values(data_dict)
        self.engine.execute(stmt)
        
    def upsert(self, data: DataFrame, tables_template: Environment, table_name: str) -> None:
        if not self.table_exists(table_name):
            self.create_table(table_name, tables_template)

        data_dict = data.to_dict(orient='records')
        table = Table(table_name, MetaData(), autoload_with=self.engine)

        # Use PostgreSQL-specific insert function for upsert capability
        stmt = insert(table).values(data_dict)

        # Preparing the on_conflict_do_update clause
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['id'],  # Assume 'id' as the conflict target
            set_={c.name: stmt.excluded[c.name] for c in table.columns if c.name != 'id'}
        )

        # Executing the upsert operation
        self.engine.execute(on_conflict_stmt)