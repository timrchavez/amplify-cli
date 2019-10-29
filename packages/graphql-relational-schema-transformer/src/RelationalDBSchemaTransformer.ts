import { Kind, ObjectTypeDefinitionNode, SchemaDefinitionNode, InputObjectTypeDefinitionNode, DocumentNode } from 'graphql';
import {
  getNamedType,
  getOperationFieldDefinition,
  getNonNullType,
  getInputValueDefinition,
  getTypeDefinition,
  getDirectiveNode,
  getOperationTypeDefinition,
  getConnectionTypeDefinition,
  getKeysTypeDefinition,
  getListType,
} from './RelationalDBSchemaTransformerUtils';
import { RelationalDBParsingException } from './RelationalDBParsingException';
import { IRelationalDBReader } from './IRelationalDBReader';
import { toPascalCase } from 'graphql-transformer-common';
import { plural } from 'pluralize';
import { string } from 'prop-types';

/**
 * This class is used to transition all of the columns and key metadata from a table for use
 * in generating appropriate GraphQL schema structures. It will track type definitions for
 * the base table, update mutation inputs, create mutation inputs, and primary key metadata.
 */
export class TableContext {
  tableTypeDefinition: ObjectTypeDefinitionNode;
  createTypeDefinition: InputObjectTypeDefinitionNode;
  updateTypeDefinition: InputObjectTypeDefinitionNode;
  // Table primary key metadata, to help properly key queries and mutations.
  tableKeyFields: string[];
  tableKeyFieldTypes: string[];
  stringFieldList: string[];
  intFieldList: string[];
  keysInputDefinition: InputObjectTypeDefinitionNode;
  relationshipData: Map<string, Map<string, string[]>[]>;
  constructor(
    typeDefinition: ObjectTypeDefinitionNode,
    createDefinition: InputObjectTypeDefinitionNode,
    updateDefinition: InputObjectTypeDefinitionNode,
    primaryKeyFields: string[],
    primaryKeyTypes: string[],
    stringFieldList: string[],
    intFieldList: string[],
    keysInputDefinition?: InputObjectTypeDefinitionNode,
    relationshipData?: Map<string, Map<string, string[]>[]>
  ) {
    this.tableTypeDefinition = typeDefinition;
    this.tableKeyFields = primaryKeyFields;
    this.createTypeDefinition = createDefinition;
    this.updateTypeDefinition = updateDefinition;
    this.tableKeyFieldTypes = primaryKeyTypes;
    this.stringFieldList = stringFieldList;
    this.intFieldList = intFieldList;
    this.keysInputDefinition = keysInputDefinition;
    this.relationshipData = relationshipData;
  }
}

/**
 * This class is used to transition all of the information needed to generate the
 * CloudFormation template. This is the class that is outputted by the SchemaTransformer and the one that
 * RelationalDBTemplateGenerator takes in for the constructor. It tracks the graphql schema document,
 * map of the primary keys for each of the types. It is also being used to track the CLI inputs needed
 * for DataSource Creation, as data source creation is apart of the cfn template generation.
 */
export class TemplateContext {
  schemaDoc: DocumentNode;
  typePrimaryKeyMap: Map<string, string[]>;
  typePrimaryKeyTypeMap: Map<string, string[]>;
  stringFieldMap: Map<string, string[]>;
  intFieldMap: Map<string, string[]>;
  secretStoreArn: string;
  rdsClusterIdentifier: string;
  databaseName: string;
  databaseSchema: string;
  region: string;
  relationshipData: Map<string, Map<string, string[]>[]>;

  constructor(
    schemaDoc: DocumentNode,
    typePrimaryKeyMap: Map<string, string[]>,
    stringFieldMap: Map<string, string[]>,
    intFieldMap: Map<string, string[]>,
    typePrimaryKeyTypeMap?: Map<string, string[]>,
    relationshipData?: Map<string, Map<string, string[]>[]>
  ) {
    this.schemaDoc = schemaDoc;
    this.stringFieldMap = stringFieldMap;
    this.intFieldMap = intFieldMap;
    this.typePrimaryKeyMap = typePrimaryKeyMap;
    this.typePrimaryKeyTypeMap = typePrimaryKeyTypeMap;
    this.relationshipData = relationshipData;
  }
}

export class RelationalDBSchemaTransformer {
  dbReader: IRelationalDBReader;
  database: string;

  constructor(dbReader: IRelationalDBReader, database: string) {
    this.dbReader = dbReader;
    this.database = database;
  }

  public introspectDatabaseSchema = async (): Promise<TemplateContext> => {
    // Get all of the tables within the provided db
    let tableNames = null;
    try {
      tableNames = await this.dbReader.listTables();
    } catch (err) {
      throw new RelationalDBParsingException(`Failed to list tables in ${this.database}`, err.stack);
    }

    let typeContexts = new Array();
    let types = new Array();
    let pkeyMap = new Map<string, string[]>();
    let pkeyTypeMap = new Map<string, string[]>();
    let stringFieldMap = new Map<string, string[]>();
    let intFieldMap = new Map<string, string[]>();
    let relationshipData = new Map<string, Map<string, string[]>[]>();

    for (const tableName of tableNames) {
      let type: TableContext = null;
      try {
        type = await this.dbReader.describeTable(tableName);
      } catch (err) {
        throw new RelationalDBParsingException(`Failed to describe table ${tableName}`, err.stack);
      }

      // NOTE from @mikeparisstuff. The GraphQL schema generation breaks
      // when the table does not have an explicit primary key.
      if (type && type.tableKeyFields.length > 0) {
        typeContexts.push(type);
        const formattedTableName = toPascalCase(tableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
        types.push(getKeysTypeDefinition(formattedTableName, type.tableKeyFields, type.tableKeyFieldTypes));
        types.push(getConnectionTypeDefinition(formattedTableName));
        // Generate the create operation input for each table type definition
        types.push(type.createTypeDefinition);
        // Generate the default shape for the table's structure
        types.push(type.tableTypeDefinition);
        // Generate the update operation input for each table type definition
        types.push(type.updateTypeDefinition);
        // Generate the keys inputs for each table type definition
        if (type.keysInputDefinition) {
          types.push(type.keysInputDefinition);
        }

        // Update the field map with the new field lists for the current table
        stringFieldMap.set(tableName, type.stringFieldList);
        intFieldMap.set(tableName, type.intFieldList);
        pkeyMap.set(tableName, type.tableKeyFields);
        pkeyTypeMap.set(tableName, type.tableKeyFieldTypes);
        relationshipData.set(tableName, type.relationshipData ? type.relationshipData.get(tableName) : [new Map(), new Map()]);
      } else {
        console.log(`Skipping table ${type.tableTypeDefinition.name.value}`);
        console.warn(`Skipping table ${type.tableTypeDefinition.name.value} because it does not have a single PRIMARY KEY.`);
      }
    }

    // Generate the mutations and queries based on the table structures
    types.push(this.getMutations(typeContexts));
    types.push(this.getQueries(typeContexts)); // timrchavez
    types.push(this.getSubscriptions(typeContexts));
    types.push(this.getSchemaType());

    let context = this.dbReader.hydrateTemplateContext(
      new TemplateContext({ kind: Kind.DOCUMENT, definitions: types }, pkeyMap, stringFieldMap, intFieldMap, pkeyTypeMap, relationshipData)
    );

    return context;
  };

  /**
   * Creates a schema type definition node, including operations for each of query, mutation, and subscriptions.
   *
   * @returns a basic schema definition node.
   */
  getSchemaType(): SchemaDefinitionNode {
    return {
      kind: Kind.SCHEMA_DEFINITION,
      directives: [],
      operationTypes: [
        getOperationTypeDefinition('query', getNamedType('Query')),
        getOperationTypeDefinition('mutation', getNamedType('Mutation')),
        getOperationTypeDefinition('subscription', getNamedType('Subscription')),
      ],
    };
  }

  /**
   * Generates the basic mutation operations, given the provided table contexts. This will
   * create a create, delete, and update operation for each table.
   *
   * @param types the table contexts from which the mutations are to be generated.
   * @returns the type definition for mutations, including a create, delete, and update for each table.
   */
  private getMutations(types: TableContext[]): ObjectTypeDefinitionNode {
    const fields = [];
    for (const typeContext of types) {
      const type = typeContext.tableTypeDefinition;
      const formattedTypeValue = toPascalCase(type.name.value.split('.'));
      fields.push(
        getOperationFieldDefinition(
          `delete${formattedTypeValue}`,
          typeContext.tableKeyFields.map((tableKeyField, pos) => {
            return getInputValueDefinition(getNonNullType(getNamedType(typeContext.tableKeyFieldTypes[pos])), tableKeyField);
          }),
          getNamedType(`${type.name.value}`),
          null
        )
      );
      fields.push(
        getOperationFieldDefinition(
          `create${formattedTypeValue}`,
          [getInputValueDefinition(getNonNullType(getNamedType(`Create${formattedTypeValue}Input`)), `create${formattedTypeValue}Input`)],
          getNamedType(`${type.name.value}`),
          null
        )
      );
      fields.push(
        getOperationFieldDefinition(
          `update${formattedTypeValue}`,
          [getInputValueDefinition(getNonNullType(getNamedType(`Update${formattedTypeValue}Input`)), `update${formattedTypeValue}Input`)],
          getNamedType(`${type.name.value}`),
          null
        )
      );
    }
    return getTypeDefinition(fields, 'Mutation');
  }

  /**
   * Generates the basic subscription operations, given the provided table contexts. This will
   * create an onCreate subscription for each table.
   *
   * @param types the table contexts from which the subscriptions are to be generated.
   * @returns the type definition for subscriptions, including an onCreate for each table.
   */
  private getSubscriptions(types: TableContext[]): ObjectTypeDefinitionNode {
    const fields = [];
    for (const typeContext of types) {
      const type = typeContext.tableTypeDefinition;
      const formattedTypeValue = toPascalCase(type.name.value.split('.'));
      fields.push(
        getOperationFieldDefinition(`onCreate${formattedTypeValue}`, [], getNamedType(`${type.name.value}`), [
          getDirectiveNode(`create${formattedTypeValue}`),
        ])
      );
    }
    return getTypeDefinition(fields, 'Subscription');
  }

  /**
   * Generates the basic query operations, given the provided table contexts. This will
   * create a get and list operation for each table.
   *
   * @param types the table contexts from which the queries are to be generated.
   * @returns the type definition for queries, including a get and list for each table.
   */
  private getQueries(types: TableContext[]): ObjectTypeDefinitionNode {
    const fields = [];
    for (const typeContext of types) {
      const type = typeContext.tableTypeDefinition;
      const formattedTypeValue = toPascalCase(type.name.value.split('.'));
      fields.push(
        getOperationFieldDefinition(
          `get${formattedTypeValue}`,
          typeContext.tableKeyFields.map((tableKeyField, pos) => {
            return getInputValueDefinition(getNonNullType(getNamedType(typeContext.tableKeyFieldTypes[pos])), tableKeyField);
          }),
          getNamedType(`${type.name.value}`),
          null
        )
      );
      fields.push(
        getOperationFieldDefinition(
          plural(`list${formattedTypeValue}`),
          [
            /* Pagination will work as follows:
               -> Listing operation returns items based on limit, sort order, etc
               -> Plus one additional row, which will be placed into nextToken
               -> nextToken will be a json object / map passed into the next Listing operation
               -> Fields to use as cursors are defined by tokenFields, which is also the sort order
               -> The corresponding fields in nextToken values will be checked based on the token field type
            */
            getInputValueDefinition(getNonNullType(getNamedType('Int')), 'limit'), // Number of items returned
            getInputValueDefinition(getNamedType(`${formattedTypeValue}KeysInput`), 'nextToken'), // The row to start from
            getInputValueDefinition(getListType('String'), 'tokenFields'), // The columns in the row to check, in order
            getInputValueDefinition(getListType('String'), 'tokenFieldTypes'), // The column types, for comparison logic
            getInputValueDefinition(getListType('String'), 'sortDirections'), // The column types, for comparison logic
            getInputValueDefinition(getNamedType('String'), 'filter'),
          ],
          getNamedType(`${type.name.value}Connection`),
          null
        )
      );
    }
    return getTypeDefinition(fields, 'Query');
  }
}
