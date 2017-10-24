import Utils from './../utils';
import { ConnectionCredentials } from './../interface/connection-credentials';
import { ConnectionDialect } from './../interface/connection-dialect';
import DatabaseInterface from './../interface/database-interface';
import { DialectQueries } from './../interface/dialect-queries';

export default class Oracle implements ConnectionDialect {

  public connection: Promise<any>;

  private queries: DialectQueries = {
    describeTable: 'SELECT * FROM user_tab_columns WHERE table_name = :table',
    fetchColumns: 'SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH FROM user_tab_columns',
    fetchRecords: 'SELECT * FROM :table LIMIT :limit',
    fetchTables: 'SELECT table_name FROM user_tables',
  } as DialectQueries;

  constructor(public credentials: ConnectionCredentials) {

  }

  public open() {
    if (this.connection) {
      return this.connection;
    }
    const { username, password, server, port, database } = this.credentials;

    const options = {
      user: username,
      password: password,
      connectString: `${server}:${port}/${database}`
    };

    const connection = require('knex')({
      client: 'oracledb',
      connection: options
    });

    return new Promise((resolve, reject) => {
      this.connection = Promise.resolve(connection);
      resolve(this.connection);
    });
  }

  public close() {
    return this.connection.then((pool) => pool.close());
  }

  public query(query: string): Promise<any> {
    return this.open().then((connection) => {
      return connection.raw(query).then(results => {
        return [results];
      });
    })
  }

  public getTables(): Promise<DatabaseInterface.Table[]> {
    return this.open().then(connection => {
      return connection.select('TABLE_NAME').from('USER_TABLES').then(results => {
        return results.map(row => {
          return { name: row.TABLE_NAME } as DatabaseInterface.Table
        }).sort();
      });
    });
  }

  public getColumns(): Promise<DatabaseInterface.TableColumn[]> {
    return this.open().then(connection => {
      return connection.select('TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'DATA_LENGTH').from('USER_TAB_COLUMNS')
      .then(results => {
        return results.map((obj) => {
          return  {
            tableName: obj.TABLE_NAME,
            columnName: obj.COLUMN_NAME,
            type: obj.DATA_TYPE,
            size: obj.DATA_LENGTHI
          } as DatabaseInterface.TableColumn;
        })
        .sort()
      })
    })
  }

  public describeTable(table: string) {
    return this.query(Utils.replacer(this.queries.describeTable, { table }));
  }

  public showRecords(table: string, limit: number = 10) {
    return this.query(Utils.replacer(this.queries.fetchRecords, {limit, table }));
  }
}
