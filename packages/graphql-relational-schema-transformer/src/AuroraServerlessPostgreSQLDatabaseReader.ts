import { TemplateContext, TableContext } from './RelationalDBSchemaTransformer';
import {
  getNamedType,
  getNonNullType,
  getListType,
  getInputValueDefinition,
  getTypeDefinition,
  getFieldDefinition,
  getInputTypeDefinition,
} from './RelationalDBSchemaTransformerUtils';
import { AuroraPostgreSQLDataAPIClient } from './AuroraPostgreSQLDataAPIClient';
import { IAuroraDataAPIClient } from './IAuroraDataAPIClient';
import { IRelationalDBReader } from './IRelationalDBReader';
import { toPascalCase, toSnakeCase } from 'graphql-transformer-common';
import { plural } from 'pluralize';
import { format } from 'prettier';

/**
 * A class to manage interactions with a Aurora Serverless MySQL Relational Databse
 * using the Aurora Data API
 */
export class AuroraServerlessPostgreSQLDatabaseReader implements IRelationalDBReader {
  auroraClient: AuroraPostgreSQLDataAPIClient;
  dbRegion: string;
  awsSecretStoreArn: string;
  dbClusterOrInstanceArn: string;
  database: string;
  schemas: string[];

  setAuroraClient(auroraClient: IAuroraDataAPIClient) {
    this.auroraClient = auroraClient;
  }

  constructor(dbRegion: string, awsSecretStoreArn: string, dbClusterOrInstanceArn: string, database: string, schemas: string[], aws: any) {
    this.auroraClient = new AuroraPostgreSQLDataAPIClient(dbRegion, awsSecretStoreArn, dbClusterOrInstanceArn, database, aws);
    this.dbRegion = dbRegion;
    this.awsSecretStoreArn = awsSecretStoreArn;
    this.dbClusterOrInstanceArn = dbClusterOrInstanceArn;
    this.database = database;
    this.schemas = schemas;
  }

  /**
   * Stores some of the Aurora Serverless MySQL context into the template context,
   * for later consumption.
   *
   * @param contextShell the basic template context, with db source independent fields set.
   * @returns a fully hydrated template context, complete with Aurora Serverless MySQL context.
   */
  hydrateTemplateContext = async (contextShell: TemplateContext): Promise<TemplateContext> => {
    /**
     * Information needed for creating the AppSync - RDS Data Source
     * Store as part of the TemplateContext
     */
    contextShell.secretStoreArn = this.awsSecretStoreArn;
    contextShell.rdsClusterIdentifier = this.dbClusterOrInstanceArn;
    /* To support multiple schemas, we refer to it in each table name, rather than specify one here */
    contextShell.databaseSchema = '';
    contextShell.databaseName = this.database;
    contextShell.region = this.dbRegion;
    return contextShell;
  };

  /**
   * Gets a list of all the table names in the provided database.
   *
   * @returns a list of tablenames inside the database.
   */
  listTables = async (): Promise<string[]> => {
    const results = await this.auroraClient.listTables(this.schemas);
    return results;
  };

  /**
   * Looks up any foreign key constraints that might exist from the provided table.
   * This is done to make solving many-to-many relationships possible.
   *
   * @param tableName the name of the table to be checked for foreign key constraints.
   * @returns a list of table names that are applicable as having constraints.
   */
  getTableForeignKeyReferences = async (tableName: string): Promise<Map<string, string[]>[]> => {
    return await this.auroraClient.getTableForeignKeyReferences(tableName);
  };

  /**
   * For the provided table, this will create a table context. That context holds definitions for
   * the base table type, the create input type, and the update input type (e.g. Post, CreatePostInput, and UpdatePostInput, respectively),
   * as well as the table primary key structure for proper operation definition.
   *
   * Create inputs will only differ from the base table type in that any nested types will not be present. Update table
   * inputs will differ in that the only required field will be the primary key/identifier, as all fields don't have to
   * be updated. Instead, it assumes the proper ones were provided on create.
   *
   * @param tableName the name of the table to be translated into a GraphQL type.
   * @returns a promise of a table context structure.
   */
  describeTable = async (tableName: string): Promise<TableContext> => {
    const columnDescriptions = await this.auroraClient.describeTable(tableName);
    // Fields in the general type (e.g. Post). Both the identifying field and any others the db dictates will be required.
    const fields = new Array();
    // Fields in the update input type (e.g. UpdatePostInput). Only the identifying field will be required, any others will be optional.
    const updateFields = new Array();
    // Field in the create input type (e.g. CreatePostInput).
    const createFields = new Array();
    // Fields in the list query type
    const keyFields = new Array();

    // Relationship information
    let relationshipData = new Map<string, Map<string, string[]>[]>();

    // The primary key (including composites), used to help generate queries and mutations
    const primaryKeys = new Array();
    const primaryKeyTypes = new Array();

    // Field Lists needed as context for auto-generating the Query Resolvers
    const intFieldList = new Array();
    const stringFieldList = new Array();
    const listFieldList = new Array();

    const [relationshipsOnOthers, relationshipsOnMe] = await this.getTableForeignKeyReferences(tableName);
    relationshipData.set(tableName, [relationshipsOnOthers, relationshipsOnMe]);

    const formattedTableName = toPascalCase(tableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
    for (const columnDescription of columnDescriptions) {
      const columnName = columnDescription.column_name;
      const dataType = columnDescription.data_type;
      const arrayType = columnDescription.array_type;
      const isPrimaryKey = columnDescription.constraint_type == 'PRIMARY KEY';
      const isNullable = columnDescription.is_nullable == 'YES';
      // Primary keys
      if (isPrimaryKey) {
        primaryKeys.push(columnName);
        primaryKeyTypes.push(getGraphQLTypeFromPostgreSQLType(dataType));
        // Foreign keys
      } else if (relationshipsOnOthers.has(columnName)) {
        /**
         * NOTE:
         * Foreign key fields are special in that they will resolve user-defined types rather than built-in GraphQL types
         */
        const foreignTableName = relationshipsOnOthers.get(columnName)[0];
        const formattedTableName = toPascalCase(foreignTableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
        const relationType = getNamedType(formattedTableName);
        const createRelationInputType = getNamedType(`Create${formattedTableName}Input`);
        const updateRelationInputType = getNamedType(`Update${formattedTableName}Input`);

        fields.push(getFieldDefinition(columnName, isNullable ? relationType : getNonNullType(relationType)));
        createFields.push(getInputValueDefinition(createRelationInputType, columnName));
        updateFields.push(
          getInputValueDefinition(isNullable ? updateRelationInputType : getNonNullType(updateRelationInputType), columnName)
        );
        // We can skip the rest if this iteration since it only pertains to mapping built-in GraphQL types onto PostgreSQL types
        continue;
        // All other fields
      } else {
        /**
         * If the field is not a key, then store it in the fields list.
         * As we need this information later to generate query resolvers
         *
         * Currently we will only auto-gen query resolvers for the Int and String scalars
         */
        const type = getGraphQLTypeFromPostgreSQLType(dataType);
        if (type === 'Int') {
          intFieldList.push(columnName);
        } else if (type === 'String') {
          stringFieldList.push(columnName);
        } else if (type === 'List') {
          listFieldList.push(columnName);
        }
      }

      // Create the basic field type shape, to be consumed by every field definition
      const baseType =
        arrayType !== 'NULL'
          ? getListType(getGraphQLTypeFromPostgreSQLType(arrayType))
          : getNamedType(getGraphQLTypeFromPostgreSQLType(dataType));

      const type = !isPrimaryKey && isNullable ? baseType : getNonNullType(baseType);
      fields.push(getFieldDefinition(columnName, type));

      createFields.push(getInputValueDefinition(type, columnName));

      const updateType = !isPrimaryKey ? baseType : getNonNullType(baseType);
      updateFields.push(getInputValueDefinition(updateType, columnName));

      if (isPrimaryKey) {
        keyFields.push(getInputValueDefinition(baseType, columnName));
      }
    }

    // Many-to-one relationships
    relationshipsOnMe.forEach(async (_, foreignTableName) => {
      const relationType = toPascalCase(foreignTableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
      const relationField = toSnakeCase(
        foreignTableName.split('.').map((word, i, arr) => {
          return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
        })
      );
      fields.push(getFieldDefinition(relationField, getListType(relationType)));
      // Many-to-many relationships
      const joinTableName = foreignTableName;
      const joinRelationField = relationField;
      const [joinTableRelationshipsOnOthers, relationshipsOnJoinTable] = await this.getTableForeignKeyReferences(joinTableName);
      relationshipData.set(joinTableName, [joinTableRelationshipsOnOthers, relationshipsOnJoinTable]);
      joinTableRelationshipsOnOthers.forEach((foreignTableData, joinColumnName) => {
        const foreignTableName = foreignTableData[0];
        if (foreignTableName !== tableName) {
          const relationType = toPascalCase(foreignTableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
          const relationField = `${toSnakeCase(
            joinColumnName.split('.').map((word, i, arr) => {
              return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
            })
          )}_via_${joinRelationField}`;
          fields.push(getFieldDefinition(relationField, getListType(relationType)));
        }
      });
    });

    return new TableContext(
      getTypeDefinition(fields, formattedTableName),
      getInputTypeDefinition(createFields, `Create${formattedTableName}Input`),
      getInputTypeDefinition(updateFields, `Update${formattedTableName}Input`),
      primaryKeys,
      primaryKeyTypes,
      stringFieldList,
      intFieldList,
      getInputTypeDefinition(keyFields, `${formattedTableName}KeysInput`),
      relationshipData
    );
  };
}

const intTypes = ['INTEGER', 'SMALLINT', 'BIGINT', 'SERIAL', 'SMALLSERIAL', 'BIGSERIAL'];
const floatTypes = ['FLOAT', 'DECIMAL', 'REAL', 'NUMERIC', 'DOUBLE PRECISION'];

/**
 * Given the PostgreSQL DB type for a column, make a best effort to select the appropriate GraphQL
 * type for the corresponding field.
 *
 * @param dbType the SQL column type.
 * @returns the GraphQL field type.
 */
export function getGraphQLTypeFromPostgreSQLType(dbType: string): string {
  const normalizedType = dbType.toUpperCase();
  if (`ARRAY` == normalizedType) {
    return `List`;
  } else if (`BOOLEAN` == normalizedType) {
    return `Boolean`;
  } else if (`JSON` == normalizedType) {
    return `AWSJSON`;
  } else if (`TIME` == normalizedType) {
    return `AWSTime`;
  } else if (`DATE` == normalizedType) {
    return `AWSDate`;
  } else if (`DATETIME` == normalizedType) {
    return `AWSDateTime`;
  } else if (`TIMESTAMP` == normalizedType || `TIMESTAMP WITHOUT TIME ZONE` == normalizedType) {
    return `AWSTimestamp`;
  } else if (`UUID` == normalizedType) {
    return `ID`;
  } else if (intTypes.indexOf(normalizedType) > -1) {
    return `Int`;
  } else if (floatTypes.indexOf(normalizedType) > -1) {
    return `Float`;
  }
  return `String`;
}
