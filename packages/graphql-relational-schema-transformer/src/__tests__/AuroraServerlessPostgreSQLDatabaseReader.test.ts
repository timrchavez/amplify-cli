import { getGraphQLTypeFromPostgreSQLType } from '../AuroraServerlessPostgreSQLDatabaseReader';
//timrc: this looks much different for postgresql
test('Test type conversion to AWSDateTime', () => {
  expect(getGraphQLTypeFromPostgreSQLType('datetime')).toEqual('AWSDateTime');
});

test('Test type conversion to AWSDate', () => {
  expect(getGraphQLTypeFromPostgreSQLType('date')).toEqual('AWSDate');
});

test('Test type conversion to AWSTime', () => {
  expect(getGraphQLTypeFromPostgreSQLType('time')).toEqual('AWSTime');
});

test('Test type conversion to AWSTimestamp', () => {
  expect(getGraphQLTypeFromPostgreSQLType('timestamp')).toEqual('AWSTimestamp');
});

test('Test type conversion to AWSJSON', () => {
  expect(getGraphQLTypeFromPostgreSQLType('jSoN')).toEqual('AWSJSON');
});

test('Test type conversion to Boolean', () => {
  expect(getGraphQLTypeFromPostgreSQLType('BOOleAn')).toEqual('Boolean');
});

test('Test type conversion to Int', () => {
  expect(getGraphQLTypeFromPostgreSQLType('InteGer')).toEqual('Int');
  expect(getGraphQLTypeFromPostgreSQLType('SmaLLInT')).toEqual('Int');
  expect(getGraphQLTypeFromPostgreSQLType('BIGINT')).toEqual('Int');
});

test('Test type conversion to Float', () => {
  expect(getGraphQLTypeFromPostgreSQLType('FloAT')).toEqual('Float');
  expect(getGraphQLTypeFromPostgreSQLType('REAL')).toEqual('Float');
  expect(getGraphQLTypeFromPostgreSQLType('DOUBLE precision')).toEqual('Float');
  expect(getGraphQLTypeFromPostgreSQLType('DeciMAL')).toEqual('Float');
  expect(getGraphQLTypeFromPostgreSQLType('Numeric')).toEqual('Float');
});

test('Test type conversion defaults to String', () => {
  expect(getGraphQLTypeFromPostgreSQLType('gibberish random stuff')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('timesta')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('boo')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('jso')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('tim')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('ate')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('atetime')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('Inte')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('Bigin')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('DECI')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('floatt')).toEqual('String');
  expect(getGraphQLTypeFromPostgreSQLType('FIXE')).toEqual('String');
});
