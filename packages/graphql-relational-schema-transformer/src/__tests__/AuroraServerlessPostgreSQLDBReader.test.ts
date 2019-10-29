import { TemplateContext, TableContext } from '../RelationalDBSchemaTransformer';
import { Kind } from 'graphql';
import { AuroraServerlessPostgreSQLDatabaseReader } from '../AuroraServerlessPostgreSQLDatabaseReader';
import { AuroraPostgreSQLDataAPIClient, PostgreSQLColumnDescription } from '../AuroraPostgreSQLDataAPIClient';
import { toUpper } from 'graphql-transformer-common';

const dbRegion = 'us-east-1';
const secretStoreArn = 'secretStoreArn';
const clusterArn = 'clusterArn';
const testDBName = 'testdb';
const testSchemaNames = ['public'];
const tableAName = 'a';
const tableBName = 'b';
const tableCName = 'c';
const tableDName = 'd';
const aws = require('aws-sdk');

const dummyReader = new AuroraServerlessPostgreSQLDatabaseReader(dbRegion, secretStoreArn, clusterArn, testDBName, testSchemaNames, aws);

test('Test describe table', async () => {
  const MockAuroraClient = jest.fn<AuroraPostgreSQLDataAPIClient>(() => ({
    describeTable: jest.fn((tableName: string) => {
      const tableColumns = [];
      const idColDescription = new PostgreSQLColumnDescription();
      const nameColDescription = new PostgreSQLColumnDescription();
      const writeACLColDescription = new PostgreSQLColumnDescription();

      idColDescription.column_name = 'id';
      idColDescription.data_type = 'integer';
      idColDescription.array_type = 'NULL';
      idColDescription.is_nullable = 'NO';
      idColDescription.constraint_type = 'PRIMARY KEY';
      idColDescription.column_default = `nextval('"${tableName}_id_seq"'::regclass)`;

      nameColDescription.column_name = 'name';
      nameColDescription.data_type = 'character varying';
      nameColDescription.array_type = 'NULL';
      nameColDescription.is_nullable = 'YES';
      nameColDescription.constraint_type = 'NULL';
      nameColDescription.column_default = 'NULL';

      writeACLColDescription.column_name = 'write_acl';
      writeACLColDescription.data_type = 'ARRAY';
      writeACLColDescription.array_type = 'integer';
      writeACLColDescription.is_nullable = 'YES';
      writeACLColDescription.constraint_type = 'NULL';
      writeACLColDescription.column_default = `'{}'::integer[]`;

      tableColumns.push(idColDescription);
      tableColumns.push(nameColDescription);
      tableColumns.push(writeACLColDescription);
      if (tableName == tableBName) {
        const foreignKeyId = new PostgreSQLColumnDescription();
        foreignKeyId.column_name = 'aId';
        foreignKeyId.data_type = 'integer';
        foreignKeyId.array_type = 'NULL';
        foreignKeyId.is_nullable = 'YES';
        foreignKeyId.constraint_type = 'NULL';
        foreignKeyId.column_default = 'NULL';

        tableColumns.push(foreignKeyId);
      }
      return tableColumns;
    }),
    getTableForeignKeyReferences: jest.fn((tableName: string) => {
      if (tableName == tableBName) {
        return [new Map([['aId', [`\\"${tableAName}\\"`, 'id']]]), new Map()];
      }
      return [new Map(), new Map()];
    }),
  }));
  const mockClient = new MockAuroraClient();
  dummyReader.setAuroraClient(mockClient);

  describeTableTestCommon(tableAName, 3, false, await dummyReader.describeTable(tableAName));
  describeTableTestCommon(tableBName, 4, true, await dummyReader.describeTable(tableBName));
  describeTableTestCommon(tableCName, 3, false, await dummyReader.describeTable(tableCName));
  describeTableTestCommon(tableDName, 3, false, await dummyReader.describeTable(tableDName));
});

function describeTableTestCommon(tableName: string, fieldLength: number, isForeignKey: boolean, tableContext: TableContext) {
  const formattedTableName = toUpper(tableName);
  expect(tableContext.tableKeyFields[0]).toEqual('id');
  expect(tableContext.tableKeyFieldTypes[0]).toEqual('Int');
  expect(tableContext.createTypeDefinition).toBeDefined();
  expect(tableContext.updateTypeDefinition).toBeDefined();
  expect(tableContext.tableTypeDefinition).toBeDefined();
  expect(tableContext.tableTypeDefinition.kind).toEqual(Kind.OBJECT_TYPE_DEFINITION);
  expect(tableContext.updateTypeDefinition.kind).toEqual(Kind.INPUT_OBJECT_TYPE_DEFINITION);
  expect(tableContext.createTypeDefinition.kind).toEqual(Kind.INPUT_OBJECT_TYPE_DEFINITION);
  expect(tableContext.tableTypeDefinition.name.value).toEqual(formattedTableName);
  expect(tableContext.tableTypeDefinition.name.kind).toEqual(Kind.NAME);
  expect(tableContext.updateTypeDefinition.name.value).toEqual(`Update${formattedTableName}Input`);
  expect(tableContext.updateTypeDefinition.name.kind).toEqual(Kind.NAME);
  expect(tableContext.createTypeDefinition.name.value).toEqual(`Create${formattedTableName}Input`);
  expect(tableContext.createTypeDefinition.name.kind).toEqual(Kind.NAME);
  expect(tableContext.tableTypeDefinition.fields.length).toEqual(fieldLength);
  expect(tableContext.updateTypeDefinition.fields.length).toEqual(fieldLength);
  expect(tableContext.createTypeDefinition.fields.length).toEqual(fieldLength);
}

test('Test hydrate template context', async () => {
  const context = await dummyReader.hydrateTemplateContext(new TemplateContext(null, null, null, null));
  expect(context.secretStoreArn).toEqual(secretStoreArn);
  expect(context.databaseName).toEqual(testDBName);
  expect(context.rdsClusterIdentifier).toEqual(clusterArn);
  expect(context.region).toEqual(dbRegion);
  expect(context.databaseSchema).toEqual('');
});

test('Test list tables', async () => {
  const MockAuroraClient = jest.fn<AuroraPostgreSQLDataAPIClient>(() => ({
    listTables: jest.fn(() => {
      return [tableAName, tableBName, tableCName, tableDName];
    }),
  }));
  const mockClient = new MockAuroraClient();
  dummyReader.setAuroraClient(mockClient);

  const tableNames = await dummyReader.listTables();
  expect(mockClient.listTables).toHaveBeenCalled();
  expect(tableNames.length).toBe(4);
  expect(tableNames.indexOf(tableAName) > -1).toBe(true);
  expect(tableNames.indexOf(tableBName) > -1).toBe(true);
  expect(tableNames.indexOf(tableCName) > -1).toBe(true);
  expect(tableNames.indexOf(tableDName) > -1).toBe(true);
});

test('Test lookup foreign key', async () => {
  const MockAuroraClient = jest.fn<AuroraPostgreSQLDataAPIClient>(() => ({
    getTableForeignKeyReferences: jest.fn((tableName: string) => {
      if (tableName == tableBName) {
        return [tableAName];
      }
      return [];
    }),
  }));
  const mockClient = new MockAuroraClient();
  dummyReader.setAuroraClient(mockClient);

  const aKeys = await dummyReader.getTableForeignKeyReferences(tableAName);
  const bKeys = await dummyReader.getTableForeignKeyReferences(tableBName);
  const cKeys = await dummyReader.getTableForeignKeyReferences(tableCName);
  const dKeys = await dummyReader.getTableForeignKeyReferences(tableDName);
  expect(aKeys).toBeDefined();
  expect(bKeys).toBeDefined();
  expect(cKeys).toBeDefined();
  expect(dKeys).toBeDefined();
  expect(aKeys.length).toBe(0);
  expect(bKeys.length).toBe(1);
  expect(cKeys.length).toBe(0);
  expect(dKeys.length).toBe(0);
  expect(bKeys[0]).toBe(tableAName);
  expect(mockClient.getTableForeignKeyReferences).toHaveBeenCalledWith(tableAName);
  expect(mockClient.getTableForeignKeyReferences).toHaveBeenCalledWith(tableBName);
  expect(mockClient.getTableForeignKeyReferences).toHaveBeenCalledWith(tableCName);
  expect(mockClient.getTableForeignKeyReferences).toHaveBeenCalledWith(tableDName);
});
