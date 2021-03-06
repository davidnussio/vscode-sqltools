import Utils from './../utils';
import mysql = require('mysql');
import { ConnectionCredentials } from './../interface/connection-credentials';
import { ConnectionDialect } from './../interface/connection-dialect';
import DatabaseInterface from './../interface/database-interface';
import { DialectQueries } from './../interface/dialect-queries';

export default class MySQL implements ConnectionDialect {
  public connection: Promise<any>;
  private queries: DialectQueries = {
    describeTable: 'DESCRIBE :table',
    fetchColumns: `SELECT TABLE_NAME AS tableName,
        COLUMN_NAME AS columnName, DATA_TYPE AS type, CHARACTER_MAXIMUM_LENGTH AS size
      FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()`,
    fetchRecords: 'SELECT * FROM :table LIMIT :limit',
    fetchTables: `SELECT TABLE_NAME as tableName
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME`,
  } as DialectQueries;
  constructor(public credentials: ConnectionCredentials) {

  }

  public open() {
    if (this.connection) {
      return this.connection;
    }
    const options = {
      database: this.credentials.database,
      host: this.credentials.server,
      multipleStatements: true,
      password: this.credentials.password,
      user: this.credentials.username,
    };
    const connection = mysql.createConnection(options);
    return new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) {
          reject('error connecting: ' + err.stack);
          return;
        }
        this.connection = Promise.resolve(connection);
        resolve(this.connection);
      });
    });
  }

  public close() {
    return this.connection.then((conn) => {
      conn.destroy();
      this.connection = null;
    });
  }

  public query(query: string): Promise<any> {
    return this.open().then((conn) => {
      return new Promise((resolve, reject) => {
        conn.query(query, (error, results) => {
          if (error) return reject(error);
          if (results.length === 0) {
            return resolve([]);
          }
          if (!Array.isArray(results[0])) {
            results = [results];
          }
          return resolve(results);
        });
      });
    }).then((results: any[]) => {
      if (results.length === 0) {
        return [];
      }
      return results;
    });
  }

  public getTables(): Promise<DatabaseInterface.Table[]> {
    return this.query(this.queries.fetchTables)
      .then((results) => {
        return results
          .reduce((prev, curr) => prev.concat(curr), [])
          .map((obj) => {
            return { name: obj.tableName } as DatabaseInterface.Table;
          })
          .sort();
      });
  }

  public getColumns(): Promise<DatabaseInterface.TableColumn[]> {
    return this.query(this.queries.fetchColumns)
      .then((results) => {
        return results
          .reduce((prev, curr) => prev.concat(curr), [])
          .map((obj) => obj as DatabaseInterface.TableColumn)
          .sort();
      });
  }

  public describeTable(table: string) {
    return this.query(Utils.replacer(this.queries.describeTable, { table }));
  }

  public showRecords(table: string, limit: number = 10) {
    return this.query(Utils.replacer(this.queries.fetchRecords, { limit, table }));
  }
}
