import { getGraphQLTypeFromMySQLType } from '../AuroraServerlessMySQLDatabaseReader';

test('Test type conversion to AWSDateTime', () => {
  expect(getGraphQLTypeFromMySQLType('datetime')).toEqual('AWSDateTime');
});

test('Test type conversion to AWSDate', () => {
  expect(getGraphQLTypeFromMySQLType('date')).toEqual('AWSDate');
});

test('Test type conversion to AWSTime', () => {
  expect(getGraphQLTypeFromMySQLType('time')).toEqual('AWSTime');
});

test('Test type conversion to AWSTimestamp', () => {
  expect(getGraphQLTypeFromMySQLType('timestamp')).toEqual('AWSTimestamp');
});

test('Test type conversion to AWSJSON', () => {
  expect(getGraphQLTypeFromMySQLType('jSoN')).toEqual('AWSJSON');
});

test('Test type conversion to Boolean', () => {
  expect(getGraphQLTypeFromMySQLType('BOOl')).toEqual('Boolean');
});

test('Test type conversion to Int', () => {
  expect(getGraphQLTypeFromMySQLType('Int')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('Int(100)')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('inteGER')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('SmaLLInT')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('TINYint')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('mediumInt')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('BIGINT')).toEqual('Int');
  expect(getGraphQLTypeFromMySQLType('BIT')).toEqual('Int');
});

test('Test type conversion to Float', () => {
  expect(getGraphQLTypeFromMySQLType('FloAT')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('DOUBle')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('REAL')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('REAL_as_FLOAT')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('DOUBLE precision')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('DEC')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('DeciMAL')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('FIXED')).toEqual('Float');
  expect(getGraphQLTypeFromMySQLType('Numeric')).toEqual('Float');
});

test('Test type conversion defaults to String', () => {
  expect(getGraphQLTypeFromMySQLType('gibberish random stuff')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('timesta')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('boo')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('jso')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('tim')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('ate')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('atetime')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('Inte')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('Bigin')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('DECI')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('floatt')).toEqual('String');
  expect(getGraphQLTypeFromMySQLType('FIXE')).toEqual('String');
});
