import { IAuroraDataAPIClient, DataApiParams } from './IAuroraDataAPIClient';

/**
 * A wrapper around the RDS data service client, forming their responses for
 * easier consumption.
 */
export class AuroraPostgreSQLDataAPIClient implements IAuroraDataAPIClient {
  AWS: any;
  RDS: any;
  Params: DataApiParams;

  setRDSClient(rdsClient: any) {
    this.RDS = rdsClient;
  }

  constructor(databaseRegion: string, awsSecretStoreArn: string, dbClusterOrInstanceArn: string, database: string, aws: any) {
    this.AWS = aws;
    this.AWS.config.update({
      region: databaseRegion,
    });

    this.RDS = new this.AWS.RDSDataService();
    this.Params = new DataApiParams();

    this.Params.secretArn = awsSecretStoreArn;
    this.Params.resourceArn = dbClusterOrInstanceArn;
    this.Params.database = database;
  }

  /**
   * Lists all of the tables in the set database.
   *
   * @param schemas the names of the schemas the tables lives under.
   * @return a list of tables in the database.
   */
  public listTables = async (schemaList: string[]) => {
    const schemaString = schemaList.length > 0 ? schemaList.map(schema => `'${schema}'`).join(',') : `''`;
    this.Params.sql = `
      SELECT
        schemaname,
        tablename,
        CURRENT_SCHEMA()
      FROM pg_catalog.pg_tables
      WHERE schemaname in (${schemaString})
    `
      .trim()
      .replace(/\s+/g, ' ');
    const response = await this.RDS.executeStatement(this.Params).promise();

    const tableList: Array<string> = [];
    const records = response['records'];
    for (const record of records) {
      // Do not include the schema name if it is the default schema
      // NOTE: This will cause problems if the default schema is changed after the resolvers are generated
      if (record[0]['stringValue'] === record[2]['stringValue']) {
        tableList.push(`\\"${record[1]['stringValue']}\\"`);
      } else {
        tableList.push(`${record[0]['stringValue']}.\\"${record[1]['stringValue']}\\"`);
      }
    }

    return tableList;
  };

  /**
   * Describes the table given, by breaking it down into individual column descriptions.
   *
   * @param tableName the name of the table to be described.
   * @return a list of column descriptions.
   */
  public describeTable = async (tableName: string) => {
    let response;

    this.Params.sql = `SELECT CURRENT_SCHEMA();`;
    response = await this.RDS.executeStatement(this.Params).promise();
    const defaultSchemaComponent = response['records'][0][0]['stringValue'];

    let [schemaComponent, tableComponent] = tableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, ''));
    if (typeof tableComponent === 'undefined') {
      tableComponent = schemaComponent;
      schemaComponent = defaultSchemaComponent;
    }

    this.Params.sql = `
      SELECT
        col.column_name,
        col.data_type,
        et.data_type as array_type,
        col.is_nullable,
        (CASE WHEN (kcu.constraint_name IS NULL) THEN kcu.constraint_name ELSE 'PRIMARY KEY' END),
        col.column_default
      FROM information_schema.columns col
      LEFT JOIN information_schema.element_types et
        ON ((col.table_catalog, col.table_schema, col.table_name, 'TABLE', col.dtd_identifier) =
            (et.object_catalog, et.object_schema, et.object_name, et.object_type, et.collection_type_identifier))
      LEFT JOIN information_schema.key_column_usage kcu
        ON ((col.table_schema, col.table_name, col.column_name) = (kcu.table_schema, kcu.table_name, kcu.column_name) AND
            kcu.constraint_name LIKE '%_pkey')
      WHERE CONCAT(col.table_schema, '.', col.table_name) = '${schemaComponent}.${tableComponent}'
      ORDER BY col.ordinal_position
    `
      .trim()
      .replace(/\s+/g, ' ');
    response = await this.RDS.executeStatement(this.Params).promise();
    const columns = response['records'];
    const columnDescriptions = [];
    for (const column of columns) {
      let colDescription = new PostgreSQLColumnDescription();
      colDescription.column_name = column[POSTGRESQL_DESCRIBE_TABLE_ORDER.column_name]['stringValue'];
      colDescription.data_type = column[POSTGRESQL_DESCRIBE_TABLE_ORDER.data_type]['stringValue'];
      colDescription.array_type =
        'isNull' in column[POSTGRESQL_DESCRIBE_TABLE_ORDER.array_type]
          ? 'NULL'
          : column[POSTGRESQL_DESCRIBE_TABLE_ORDER.array_type]['stringValue'];
      colDescription.is_nullable = column[POSTGRESQL_DESCRIBE_TABLE_ORDER.is_nullable]['stringValue'];
      colDescription.constraint_type =
        'isNull' in column[POSTGRESQL_DESCRIBE_TABLE_ORDER.constraint_type]
          ? 'NULL'
          : column[POSTGRESQL_DESCRIBE_TABLE_ORDER.constraint_type]['stringValue'];
      colDescription.column_default =
        'isNull' in column[POSTGRESQL_DESCRIBE_TABLE_ORDER.column_default]
          ? 'NULL'
          : column[POSTGRESQL_DESCRIBE_TABLE_ORDER.column_default]['stringValue'];
      columnDescriptions.push(colDescription);
    }
    return columnDescriptions;
  };

  /**
   * Gets foreign keys for the given table, if any exist.
   *
   * @param tableName the name of the table to be checked.
   * @return a list of tables referencing the provided table, if any exist.
   */
  public getTableForeignKeyReferences = async (tableName: string) => {
    let response;

    this.Params.sql = `SELECT CURRENT_SCHEMA();`;
    response = await this.RDS.executeStatement(this.Params).promise();
    const defaultSchemaComponent = response['records'][0][0]['stringValue'];

    let [schemaComponent, tableComponent] = tableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, ''));
    if (typeof tableComponent === 'undefined') {
      tableComponent = schemaComponent;
      schemaComponent = defaultSchemaComponent;
    }

    const relationshipsOnOthers = new Map<string, string[]>();
    this.Params.sql = `
      SELECT
        kcu.column_name,
        ccu.column_name AS foreign_column_name,
        ccu.table_schema as foreign_table_schema,
        ccu.table_name AS foreign_table_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
      WHERE constraint_type = 'FOREIGN KEY' AND CONCAT(tc.table_schema, '.', tc.table_name) = '${schemaComponent}.${tableComponent}'
    `
      .trim()
      .replace(/\s+/g, ' ');
    response = await this.RDS.executeStatement(this.Params).promise();
    for (const record of response['records']) {
      let rightTableName;
      const columnName = record[0]['stringValue'];
      // Do not include the schema name if it is the default schema
      // NOTE: This will cause problems if the default schema is changed after the resolvers are generated
      if (record[2]['stringValue'] === defaultSchemaComponent) {
        rightTableName = `\\"${record[3]['stringValue']}\\"`;
      } else {
        rightTableName = `${record[2]['stringValue']}.\\"${record[3]['stringValue']}\\"`;
      }
      const rightTableColumnName = record[1]['stringValue'];
      relationshipsOnOthers.set(columnName, [rightTableName, rightTableColumnName]);
    }

    const relationshipsOnMe = new Map<string, string[]>();
    this.Params.sql = `
      SELECT
        kcu1.table_schema,
        kcu1.table_name,
        kcu2.column_name,
        kcu1.column_name
      FROM information_schema.key_column_usage kcu1
      JOIN information_schema.referential_constraints fk USING (constraint_schema, constraint_name)
      JOIN information_schema.key_column_usage kcu2
        ON kcu2.constraint_schema = fk.unique_constraint_schema
          AND kcu2.constraint_name = fk.unique_constraint_name
          AND kcu2.ordinal_position = kcu1.position_in_unique_constraint
      WHERE CONCAT(kcu2.table_schema, '.', kcu2.table_name) = '${schemaComponent}.${tableComponent}'
    `
      .trim()
      .replace(/\s+/g, ' ');
    response = await this.RDS.executeStatement(this.Params).promise();
    for (const record of response['records']) {
      const leftTableColumnName = record[2]['stringValue'];
      const rightTableColumnName = record[3]['stringValue'];
      let rightTableName;
      if (record[0]['stringValue'] === defaultSchemaComponent) {
        rightTableName = `\\"${record[1]['stringValue']}\\"`;
      } else {
        rightTableName = `${record[0]['stringValue']}.\\"${record[1]['stringValue']}\\"`;
      }
      relationshipsOnMe.set(rightTableName, [leftTableColumnName, rightTableColumnName]);
    }

    return [relationshipsOnOthers, relationshipsOnMe];
  };
}

export class PostgreSQLColumnDescription {
  column_name: string;
  data_type: string;
  array_type: string;
  is_nullable: string;
  constraint_type: string;
  column_default: string;
}

enum POSTGRESQL_DESCRIBE_TABLE_ORDER {
  column_name,
  data_type,
  array_type,
  is_nullable,
  constraint_type,
  column_default,
}
