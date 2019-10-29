import { AliasRoutingConfiguration } from 'cloudform-types/types/lambda/alias';

export interface IAuroraDataAPIClient {
  AWS: any;
  RDS: any;
  Params: DataApiParams;
  setRDSClient(rdsClient: any): void;
  listTables(tableSchemas?: string[]): Promise<string[]>;
  describeTable(tableName: string): Promise<string[]>;
  getTableForeignKeyReferences(tableName: string): Promise<Map<string, string[]>[]>;
}

export class DataApiParams {
  database: string;
  secretArn: string;
  resourceArn: string;
  sql: string;
  includeResultMetadata?: boolean;
}
