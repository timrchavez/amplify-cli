import { AuroraPostgreSQLDataAPIClient } from '../AuroraPostgreSQLDataAPIClient';
import { DataApiParams } from '../IAuroraDataAPIClient';

const region = 'us-east-1';
const secretStoreArn = 'secretStoreArn';
const clusterArn = 'clusterArn';
const databaseName = 'Animals';
const tableAName = 'Dog';
const tableBName = 'other_schema."Owners"';

test('Test list tables', async () => {
  const rdsPromise = {
    promise: jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        const response = {
          numberOfRecordsUpdated: 0,
          records: [
            [{ stringValue: 'public' }, { stringValue: 'Dog' }, { stringValue: 'public' }],
            [{ stringValue: 'other_schema' }, { stringValue: 'Owners' }, { stringValue: 'public' }],
          ],
        };
        resolve(response);
      });
    }),
  };
  const MockRDSClient = jest.fn<any>(() => ({
    executeStatement: jest.fn((params: DataApiParams) => {
      return rdsPromise;
    }),
  }));

  const aws = require('aws-sdk');
  const testClient = new AuroraPostgreSQLDataAPIClient(region, secretStoreArn, clusterArn, databaseName, aws);
  const mockRDS = new MockRDSClient();
  testClient.setRDSClient(mockRDS);

  const tables = await testClient.listTables(['public', 'other_schema']);
  const Params = new DataApiParams();
  Params.secretArn = secretStoreArn;
  Params.resourceArn = clusterArn;
  Params.database = databaseName;
  Params.sql = `
    SELECT
      schemaname,
      tablename,
      CURRENT_SCHEMA()
    FROM pg_catalog.pg_tables
    WHERE schemaname in ('public','other_schema')
  `
    .trim()
    .replace(/\s+/g, ' ');
  expect(mockRDS.executeStatement).toHaveBeenCalledWith(Params);
  expect(tables.length).toEqual(2);
  expect(tables[0]).toEqual(`\\\"${tableAName}\\\"`);
});

test('Test foreign key lookup', async () => {
  const [schemaBComponent, tableBComponent] = tableBName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, ''));

  const rdsPromiseOne = {
    promise: jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        const response = {
          records: [[{ stringValue: 'public' }]],
        };
        resolve(response);
      });
    }),
  };

  const rdsPromiseTwo = {
    promise: jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        const response = {
          numberOfRecordsUpdated: 0,
          records: [[{ stringValue: 'aId' }, { stringValue: 'id' }, { stringValue: 'public' }, { stringValue: tableAName }]],
        };
        resolve(response);
      });
    }),
  };

  const rdsPromiseThree = {
    promise: jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        const response = {
          records: [[{ stringValue: 'other_schema' }, { stringValue: tableBComponent }, { stringValue: 'id' }, { stringValue: 'aId' }]],
        };
        resolve(response);
      });
    }),
  };

  let firstReturned = false;
  let secondReturned = false;
  const MockRDSClient = jest.fn<any>(() => ({
    executeStatement: jest.fn((params: DataApiParams) => {
      if (!firstReturned) {
        firstReturned = true;
        return rdsPromiseOne;
      } else if (!secondReturned) {
        secondReturned = true;
        return rdsPromiseTwo;
      } else {
        return rdsPromiseThree;
      }
    }),
  }));

  const aws = require('aws-sdk');
  const testClient = new AuroraPostgreSQLDataAPIClient(region, secretStoreArn, clusterArn, databaseName, aws);
  const mockRDS = new MockRDSClient();
  testClient.setRDSClient(mockRDS);

  const [relationshipsOnOthers, relationshipsOnMe] = await testClient.getTableForeignKeyReferences(tableBName);
  const Params = new DataApiParams();
  Params.secretArn = secretStoreArn;
  Params.resourceArn = clusterArn;
  Params.database = databaseName;
  Params.sql = `
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
  WHERE CONCAT(kcu2.table_schema, '.', kcu2.table_name) = '${schemaBComponent}.${tableBComponent}'
  `
    .trim()
    .replace(/\s+/g, ' ');
  expect(mockRDS.executeStatement).toHaveBeenCalledWith(Params);
  expect(relationshipsOnOthers.size).toEqual(1);
  expect(relationshipsOnMe.size).toEqual(1);
  expect(relationshipsOnOthers.keys().next().value).toEqual('aId');
  expect(relationshipsOnOthers.values().next().value).toEqual([`\\"${tableAName}\\"`, 'id']);
  expect(relationshipsOnMe.keys().next().value).toEqual(`other_schema.\\\"${tableBComponent}\\\"`);
  expect(relationshipsOnMe.values().next().value).toEqual(['id', 'aId']);
});

test('Test describe table', async () => {
  const rdsPromiseOne = {
    promise: jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        const response = {
          records: [[{ stringValue: 'public' }]],
        };
        resolve(response);
      });
    }),
  };

  const rdsPromiseTwo = {
    promise: jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        const response = {
          numberOfRecordsUpdated: -1,
          records: [
            [
              { stringValue: 'id' },
              { stringValue: 'integer' },
              { isNull: true },
              { stringValue: 'NO' },
              { stringValue: 'PRIMARY KEY' },
              { stringValue: `nextval("${tableAName}_id_seq"'::regclass)` },
            ],
            [
              { stringValue: 'created_at' },
              { stringValue: 'timestamp without time zone' },
              { isNull: true },
              { stringValue: 'YES' },
              { isNull: true },
              { stringValue: 'CURRENT_TIMESTAMP' },
            ],
            [
              { stringValue: 'acl_write' },
              { stringValue: 'ARRAY' },
              { stringValue: 'uuid' },
              { stringValue: 'YES' },
              { isNull: true },
              { stringValue: "'{}'::uuid[]" },
            ],
          ],
        };
        resolve(response);
      });
    }),
  };

  let valueReturned = false;
  const MockRDSClient = jest.fn<any>(() => ({
    executeStatement: jest.fn((params: DataApiParams) => {
      if (!valueReturned) {
        valueReturned = true;
        return rdsPromiseOne;
      } else {
        return rdsPromiseTwo;
      }
    }),
  }));

  const aws = require('aws-sdk');
  const testClient = new AuroraPostgreSQLDataAPIClient(region, secretStoreArn, clusterArn, databaseName, aws);
  const mockRDS = new MockRDSClient();
  testClient.setRDSClient(mockRDS);

  const columnDescriptions = await testClient.describeTable(tableAName);
  const Params = new DataApiParams();
  Params.secretArn = secretStoreArn;
  Params.resourceArn = clusterArn;
  Params.database = databaseName;
  Params.sql = `
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
  WHERE CONCAT(col.table_schema, '.', col.table_name) = 'public.${tableAName}'
  ORDER BY col.ordinal_position
  `
    .trim()
    .replace(/\s+/g, ' ');
  expect(mockRDS.executeStatement).toHaveBeenCalledWith(Params);
  expect(columnDescriptions.length).toEqual(3);
  expect(columnDescriptions[0].data_type).toEqual('integer');
  expect(columnDescriptions[1].data_type).toEqual('timestamp without time zone');
  expect(columnDescriptions[2].data_type).toEqual('ARRAY');
  expect(columnDescriptions[2].array_type).toEqual('uuid');
  // TODO: the rest of these tests
});
