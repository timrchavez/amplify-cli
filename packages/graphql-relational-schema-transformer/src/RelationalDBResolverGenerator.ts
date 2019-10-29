import { TemplateContext } from './RelationalDBSchemaTransformer';
import { DocumentNode } from 'graphql';
import { Fn } from 'cloudform-types';
import AppSync from 'cloudform-types/types/appSync';
import {
  print,
  obj,
  set,
  str,
  list,
  forEach,
  ref,
  compoundExpression,
  ifElse,
  raw,
  iff,
  equals,
  qref,
  Expression,
} from 'graphql-mapping-template';
import { graphqlName, toPascalCase, toCamelCase, toSnakeCase } from 'graphql-transformer-common';
import { ResourceConstants } from './ResourceConstants';
import { RelationalDBMappingTemplate } from './RelationalDBMappingTemplate';
import * as fs from 'fs-extra';
import { plural } from 'pluralize';
import { getFieldDefinition, getListType } from './RelationalDBSchemaTransformerUtils';

const s3BaseUrl = 's3://${S3DeploymentBucket}/${S3DeploymentRootKey}/resolvers/${ResolverFileName}';
const resolverFileName = 'ResolverFileName';
/**
 * This Class is responsible for Generating the RDS Resolvers based on the
 * GraphQL Schema + Metadata of the RDS Cluster (i.e. Primary Keys for Tables).
 *
 * It will generate the CRUDL+Q (Create, Retrieve, Update, Delete, List + Queries) Resolvers as
 * Cloudform Resources so that they may be added on to the base template that the
 * RelationDBTemplateGenerator creates.
 */
export class RelationalDBResolverGenerator {
  document: DocumentNode;
  typePrimaryKeyMap: Map<string, string[]>;
  stringFieldMap: Map<string, string[]>;
  intFieldMap: Map<string, string[]>;
  resolverFilePath: string;
  typePrimaryKeyTypeMap: Map<string, string[]>;
  relationshipData: Map<string, Map<string, string[]>[]>;

  constructor(context: TemplateContext) {
    this.document = context.schemaDoc;
    this.typePrimaryKeyMap = context.typePrimaryKeyMap;
    this.stringFieldMap = context.stringFieldMap;
    this.intFieldMap = context.intFieldMap;
    this.typePrimaryKeyTypeMap = context.typePrimaryKeyTypeMap;
    this.relationshipData = context.relationshipData;
  }

  /**
   * Creates the CRUDL+Q Resolvers as a Map of Cloudform Resources. The output can then be
   * merged with an existing Template's map of Resources.
   */
  public createRelationalResolvers(resolverFilePath: string) {
    let resources = {};
    this.resolverFilePath = resolverFilePath;
    this.typePrimaryKeyMap.forEach((_, tableName: string) => {
      const resourceName = toPascalCase(tableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
      resources = {
        ...resources,
        ...{ [resourceName + 'CreateResolver']: this.makeCreateRelationalResolver(tableName) },
        ...{ [resourceName + 'GetResolver']: this.makeGetRelationalResolver(tableName) },
        ...{ [resourceName + 'UpdateResolver']: this.makeUpdateRelationalResolver(tableName) },
        ...{ [resourceName + 'DeleteResolver']: this.makeDeleteRelationalResolver(tableName) },
        ...{ [resourceName + 'ListResolver']: this.makeListRelationalResolver(tableName) },
      };
      // TODO: Add Guesstimate Query Resolvers
    });

    return resources;
  }

  /**
   * Private Helpers to Generate the CFN Spec for the Resolver Resources
   */

  /**
   * Creates and returns the CFN Spec for the 'Create' Resolver Resource provided
   * a GraphQL Type as the input
   *
   * @param type - the graphql type for which the create resolver will be created
   * @param mutationTypeName - will be 'Mutation'
   */
  private makeCreateRelationalResolver(type: string, mutationTypeName: string = 'Mutation') {
    const sqlFilters = [];
    const formattedType = toPascalCase(type.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
    this.typePrimaryKeyMap.get(type).forEach((col, pos) => {
      if (this.typePrimaryKeyTypeMap.get(type)[pos].includes('ID') || this.typePrimaryKeyTypeMap.get(type)[pos].includes('String')) {
        sqlFilters.push(`${col}=\'$ctx.args.create${formattedType}Input.${col}\'`);
      } else {
        sqlFilters.push(`${col}=$ctx.args.create${formattedType}Input.${col}`);
      }
    });
    let selectSql = `SELECT * FROM ${type} WHERE ${sqlFilters.join(' AND ')}`;
    let createSql = `INSERT INTO ${type} $colStr VALUES $valStr`;

    const fieldName = graphqlName('create' + formattedType);
    const reqFileName = `${mutationTypeName}.${fieldName}.req.vtl`;
    const resFileName = `${mutationTypeName}.${fieldName}.res.vtl`;

    const reqTemplate = print(
      compoundExpression([
        set(ref('cols'), list([])),
        set(ref('vals'), list([])),
        forEach(ref('entry'), ref(`ctx.args.create${formattedType}Input.keySet()`), [
          set(ref('discard'), ref(`cols.add($entry)`)),
          set(ref('discard'), ref(`vals.add("'$ctx.args.create${formattedType}Input[$entry]'")`)),
        ]),
        set(ref('valStr'), ref('vals.toString().replace("[","(").replace("]",")")')),
        set(ref('colStr'), ref('cols.toString().replace("[","(").replace("]",")")')),
        RelationalDBMappingTemplate.rdsQuery({
          statements: list([str(createSql), str(selectSql)]),
        }),
      ])
    );

    const resTemplate = print(ref('utils.toJson($utils.parseJson($utils.rds.toJsonString($ctx.result))[1][0])'));

    fs.writeFileSync(`${this.resolverFilePath}/${reqFileName}`, reqTemplate, 'utf8');
    fs.writeFileSync(`${this.resolverFilePath}/${resFileName}`, resTemplate, 'utf8');

    let resolver = new AppSync.Resolver({
      ApiId: Fn.Ref(ResourceConstants.PARAMETERS.AppSyncApiId),
      DataSourceName: Fn.GetAtt(ResourceConstants.RESOURCES.RelationalDatabaseDataSource, 'Name'),
      TypeName: mutationTypeName,
      FieldName: fieldName,
      RequestMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: reqFileName,
      }),
      ResponseMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: resFileName,
      }),
    }).dependsOn([ResourceConstants.RESOURCES.RelationalDatabaseDataSource]);
    return resolver;
  }

  /**
   * Creates and Returns the CFN Spec for the 'Get' Resolver Resource provided
   * a GraphQL type
   *
   * NOTE: This will resolve a Connection field one level deep.
   * Open Questions:
   * - Do we need to support arbitrary nesting with JOINs?
   * - Do we really want to return an aggregated JSON object here or;
   * - Do we want to return a Connections object? If so, how would we
   *   use 'limit' and the pagination stuff?
   *
   * @param type - the graphql type for which the get resolver will be created
   * @param queryTypeName  - will be 'Query'
   */
  private makeGetRelationalResolver(type: string, queryTypeName: string = 'Query') {
    const sqlJoins = [];
    const sqlSelects = [];
    const sqlFilters = [];
    const sqlGroupBy = [];
    const relFields = [];

    const leftTableName = type;
    const leftTableType = toPascalCase(leftTableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
    const leftTableAlias = toSnakeCase(
      leftTableName.split('.').map((word, i, arr) => {
        return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
      })
    );

    sqlSelects.push(`${leftTableAlias}.*`);

    this.typePrimaryKeyMap.get(leftTableName).forEach((columnName, pos) => {
      if (
        this.typePrimaryKeyTypeMap.get(leftTableName)[pos].includes('ID') ||
        this.typePrimaryKeyTypeMap.get(leftTableName)[pos].includes('String')
      ) {
        sqlFilters.push(`${leftTableAlias}.${columnName}=\'$ctx.args.${columnName}\'`);
      } else {
        sqlFilters.push(`${leftTableAlias}.${columnName}=$ctx.args.${columnName}`);
      }
      sqlGroupBy.push(`${leftTableAlias}.${columnName}`);
    });

    const [relationshipsOnOthers, relationshipsOnMe] = this.relationshipData
      ? this.relationshipData.get(leftTableName)
      : [new Map(), new Map()];

    // One-to-one relationships (e.g. resolves my explicit references; a -> b)
    relationshipsOnOthers.forEach((foreignTableData, columnName) => {
      const [foreignTableName, foreignTableColumnName] = foreignTableData;
      const foreignTableAlias = toSnakeCase(
        columnName.split('.').map(word => {
          return word.replace(/[^A-Za-z0-9]/g, '');
        })
      );
      sqlJoins.push(
        `LEFT JOIN ${foreignTableName} AS ${foreignTableAlias} ON ${foreignTableAlias}.${foreignTableColumnName}=${leftTableAlias}.${columnName}`
      );
      sqlSelects.push(
        `CASE WHEN COUNT(${foreignTableAlias}.*) = 0 THEN '[]' ELSE json_agg(${foreignTableAlias}.*) END AS ${foreignTableAlias}`
      );
      relFields.push(foreignTableAlias);
    });

    // Many-to-one relationships (e.g. resolves foreign references on me; a <- b)
    relationshipsOnMe.forEach((columnData, foreignTableName) => {
      const [columnName, foreignTableColumnName] = columnData;
      const foreignTableAlias = toSnakeCase(
        foreignTableName.split('.').map((word, i, arr) => {
          return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
        })
      );
      sqlJoins.push(
        `LEFT JOIN ${foreignTableName} AS ${foreignTableAlias} ON ${foreignTableAlias}.${foreignTableColumnName}=${leftTableAlias}.${columnName}`
      );
      sqlSelects.push(
        `CASE WHEN COUNT(${foreignTableAlias}.*) = 0 THEN '[]' ELSE json_agg(${foreignTableAlias}.*) END AS ${foreignTableAlias}`
      );
      relFields.push(foreignTableAlias);
      const joinTableName = foreignTableName;
      const joinTableAlias = foreignTableAlias;
      const [joinTableRelationships, _] = this.relationshipData.get(joinTableName);
      // If this table only has one relationship, it cannot, by definition, be a join table
      if (joinTableRelationships.size == 1) {
        return;
      }
      // Many-to-many relationships (resolves my implicit references via an intermediary; a -> c via a <- b -> c)
      joinTableRelationships.forEach((foreignTableData, joinTableColumnName) => {
        const [foreignTableName, foreignTableColumnName] = foreignTableData;
        if (foreignTableName !== leftTableName) {
          const rightTableName = foreignTableName;
          const rightTableColumnName = foreignTableColumnName;
          const rightTableAlias = `${toSnakeCase(
            joinTableColumnName.split('.').map((word, i, arr) => {
              return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
            })
          )}_via_${joinTableAlias}`;
          sqlJoins.push(
            `LEFT JOIN ${rightTableName} AS ${rightTableAlias} ON ${rightTableAlias}.${rightTableColumnName}=${joinTableAlias}.${joinTableColumnName}`
          );
          sqlSelects.push(
            `CASE WHEN COUNT(${rightTableAlias}.*) = 0 THEN '[]' ELSE json_agg(${rightTableAlias}.*) END AS ${rightTableAlias}`
          );
          relFields.push(rightTableAlias);
        }
      });
    });

    let sql = `SELECT ${sqlSelects.join(',')} FROM ${leftTableName} ${leftTableAlias} ${sqlJoins.join(' ')} WHERE ${sqlFilters.join(
      ' AND '
    )} GROUP BY ${sqlGroupBy.join(',')}`;

    const fieldName = graphqlName('get' + leftTableType);
    const reqFileName = `${queryTypeName}.${fieldName}.req.vtl`;
    const resFileName = `${queryTypeName}.${fieldName}.res.vtl`;

    const reqTemplate = print(
      compoundExpression([
        RelationalDBMappingTemplate.rdsQuery({
          statements: list([str(sql)]),
        }),
      ])
    );

    const expression = new Array<Expression>(set(ref('result'), ref('utils.rds.toJsonObject($ctx.result)[0][0]')))
      .concat(
        // Important! Because we are returning aggregated JSON objects for relationship fields, we need to parse them
        // before returning the response
        relFields.map(relField => {
          return set(ref(`result['${relField}']`), ref(`util.parseJson($result['${relField}'])`));
        })
      )
      .concat(ref('utils.toJson($result)'));

    const resTemplate = print(compoundExpression(expression));

    fs.writeFileSync(`${this.resolverFilePath}/${reqFileName}`, reqTemplate, 'utf8');
    fs.writeFileSync(`${this.resolverFilePath}/${resFileName}`, resTemplate, 'utf8');

    let resolver = new AppSync.Resolver({
      ApiId: Fn.Ref(ResourceConstants.PARAMETERS.AppSyncApiId),
      DataSourceName: Fn.GetAtt(ResourceConstants.RESOURCES.RelationalDatabaseDataSource, 'Name'),
      FieldName: fieldName,
      TypeName: queryTypeName,
      RequestMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: reqFileName,
      }),
      ResponseMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: resFileName,
      }),
    }).dependsOn([ResourceConstants.RESOURCES.RelationalDatabaseDataSource]);
    return resolver;
  }

  /**
   * Creates and Returns the CFN Spec for the 'Update' Resolver Resource provided
   * a GraphQL type
   *
   * @param type - the graphql type for which the update resolver will be created
   * @param mutationTypeName - will be 'Mutation'
   */
  private makeUpdateRelationalResolver(type: string, mutationTypeName: string = 'Mutation') {
    const tableName = type;
    const formattedType = toPascalCase(tableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));

    let sqlFilters = [];
    this.typePrimaryKeyMap.get(tableName).forEach((col, pos) => {
      if (this.typePrimaryKeyTypeMap.get(type)[pos].includes('ID') || this.typePrimaryKeyTypeMap.get(type)[pos].includes('String')) {
        sqlFilters.push(`${col}=\'$ctx.args.update${formattedType}Input.${col}\'`);
      } else {
        sqlFilters.push(`${col}=$ctx.args.update${formattedType}Input.${col}`);
      }
    });
    const updateSql = `UPDATE ${type} SET $update WHERE ${sqlFilters.join(' AND ')}`;
    const selectSql = `SELECT * FROM ${type} WHERE ${sqlFilters.join(' AND ')}`;

    const fieldName = graphqlName('update' + formattedType);
    const reqFileName = `${mutationTypeName}.${fieldName}.req.vtl`;
    const resFileName = `${mutationTypeName}.${fieldName}.res.vtl`;

    const reqTemplate = print(
      compoundExpression([
        set(ref('updateList'), obj({})),
        forEach(ref('entry'), ref(`ctx.args.update${formattedType}Input.keySet()`), [
          set(ref('discard'), ref(`updateList.put($entry, "'$ctx.args.update${formattedType}Input[$entry]'")`)),
        ]),
        set(ref('update'), ref(`updateList.toString().replace("{","").replace("}","")`)),
        RelationalDBMappingTemplate.rdsQuery({
          statements: list([str(updateSql), str(selectSql)]),
        }),
      ])
    );

    const resTemplate = print(ref('utils.toJson($utils.parseJson($utils.rds.toJsonString($ctx.result))[1][0])'));

    fs.writeFileSync(`${this.resolverFilePath}/${reqFileName}`, reqTemplate, 'utf8');
    fs.writeFileSync(`${this.resolverFilePath}/${resFileName}`, resTemplate, 'utf8');

    let resolver = new AppSync.Resolver({
      ApiId: Fn.Ref(ResourceConstants.PARAMETERS.AppSyncApiId),
      DataSourceName: Fn.GetAtt(ResourceConstants.RESOURCES.RelationalDatabaseDataSource, 'Name'),
      TypeName: mutationTypeName,
      FieldName: fieldName,
      RequestMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: reqFileName,
      }),
      ResponseMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: resFileName,
      }),
    }).dependsOn([ResourceConstants.RESOURCES.RelationalDatabaseDataSource]);
    return resolver;
  }

  /**
   * Creates and Returns the CFN Spec for the 'Delete' Resolver Resource provided
   * a GraphQL type
   *
   * @param type - the graphql type for which the delete resolver will be created
   * @param mutationTypeName - will be 'Mutation'
   */
  private makeDeleteRelationalResolver(type: string, mutationTypeName: string = 'Mutation') {
    const formattedType = toPascalCase(type.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));

    let sqlFilters = [];
    this.typePrimaryKeyMap.get(type).forEach((col, pos) => {
      if (this.typePrimaryKeyTypeMap.get(type)[pos].includes('ID') || this.typePrimaryKeyTypeMap.get(type)[pos].includes('String')) {
        sqlFilters.push(`${col}=\'$ctx.args.${col}\'`);
      } else {
        sqlFilters.push(`${col}=$ctx.args.${col}`);
      }
    });
    const selectSql = `SELECT * FROM ${type} WHERE ${sqlFilters.join(' AND ')}`;

    sqlFilters = [];
    this.typePrimaryKeyMap.get(type).forEach((col, pos) => {
      if (this.typePrimaryKeyTypeMap.get(type)[pos].includes('ID') || this.typePrimaryKeyTypeMap.get(type)[pos].includes('String')) {
        sqlFilters.push(`${col}=\'$ctx.args.${col}\'`);
      } else {
        sqlFilters.push(`${col}=$ctx.args.${col}`);
      }
    });
    const deleteSql = `DELETE FROM ${type} WHERE ${sqlFilters.join(' AND ')}`;

    const fieldName = graphqlName('delete' + formattedType);
    const reqFileName = `${mutationTypeName}.${fieldName}.req.vtl`;
    const resFileName = `${mutationTypeName}.${fieldName}.res.vtl`;
    const reqTemplate = print(
      compoundExpression([
        RelationalDBMappingTemplate.rdsQuery({
          statements: list([str(selectSql), str(deleteSql)]),
        }),
      ])
    );
    const resTemplate = print(ref('utils.toJson($utils.rds.toJsonObject($ctx.result)[0][0])'));

    fs.writeFileSync(`${this.resolverFilePath}/${reqFileName}`, reqTemplate, 'utf8');
    fs.writeFileSync(`${this.resolverFilePath}/${resFileName}`, resTemplate, 'utf8');

    let resolver = new AppSync.Resolver({
      ApiId: Fn.Ref(ResourceConstants.PARAMETERS.AppSyncApiId),
      DataSourceName: Fn.GetAtt(ResourceConstants.RESOURCES.RelationalDatabaseDataSource, 'Name'),
      TypeName: mutationTypeName,
      FieldName: fieldName,
      RequestMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: reqFileName,
      }),
      ResponseMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: resFileName,
      }),
    }).dependsOn([ResourceConstants.RESOURCES.RelationalDatabaseDataSource]);

    return resolver;
  }

  /**
   * Creates and Returns the CFN Spec for the 'List' Resolver Resource provided
   * a GraphQL type
   *
   * @param type - the graphql type for which the list resolver will be created
   * @param queryTypeName - will be 'Query'
   */
  private makeListRelationalResolver(type: string, queryTypeName: string = 'Query') {
    const sqlJoins = [];
    const sqlSelects = [];
    const sqlGroupBy = [];
    const sqlOrderBy = [];
    const relFields = [];

    const leftTableName = type;
    const leftTableType = toPascalCase(leftTableName.split('.').map(word => word.replace(/[^A-Za-z0-9]/g, '')));
    const leftTableAlias = toSnakeCase(
      leftTableName.split('.').map((word, i, arr) => {
        return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
      })
    );

    const defaultPageLimit = 10;
    const defaultSortDirections = new Array();
    const defaultTokenFields = this.typePrimaryKeyMap.get(type);
    const defaultTokenFieldTypes = this.typePrimaryKeyTypeMap.get(type);

    const fieldName = plural(graphqlName('list' + leftTableType));
    const reqFileName = `${queryTypeName}.${fieldName}.req.vtl`;
    const resFileName = `${queryTypeName}.${fieldName}.res.vtl`;

    defaultTokenFields.forEach(tokenField => {
      defaultSortDirections.push('ASC');
    });

    sqlSelects.push(`${leftTableAlias}.*`);

    defaultTokenFields.forEach(columnName => {
      sqlGroupBy.push(`${leftTableAlias}.${columnName}`);
      sqlOrderBy.push(`${leftTableAlias}.${columnName}`);
    });

    const [relationshipsOnOthers, relationshipsOnMe] = this.relationshipData
      ? this.relationshipData.get(leftTableName)
      : [new Map(), new Map()];

    // One-to-one relationships (e.g. resolves my explicit references; a -> b)
    relationshipsOnOthers.forEach((foreignTableData, columnName) => {
      const [foreignTableName, foreignTableColumnName] = foreignTableData;
      const foreignTableAlias = toSnakeCase(
        columnName.split('.').map(word => {
          return word.replace(/[^A-Za-z0-9]/g, '');
        })
      );
      sqlJoins.push(
        `LEFT JOIN ${foreignTableName} AS ${foreignTableAlias} ON ${foreignTableAlias}.${foreignTableColumnName}=${leftTableAlias}.${columnName}`
      );
      sqlSelects.push(
        `CASE WHEN COUNT(${foreignTableAlias}.*) = 0 THEN ''[]'' ELSE json_agg(${foreignTableAlias}.*) END AS ${foreignTableAlias}`
      );
      relFields.push(foreignTableAlias);
    });

    // Many-to-one relationships (e.g. resolves foreign references on me; a <- b)
    relationshipsOnMe.forEach((columnData, foreignTableName) => {
      const [columnName, foreignTableColumnName] = columnData;
      const foreignTableAlias = toSnakeCase(
        foreignTableName.split('.').map((word, i, arr) => {
          return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
        })
      );
      sqlJoins.push(
        `LEFT JOIN ${foreignTableName} AS ${foreignTableAlias} ON ${foreignTableAlias}.${foreignTableColumnName}=${leftTableAlias}.${columnName}`
      );
      sqlSelects.push(
        `CASE WHEN COUNT(${foreignTableAlias}.*) = 0 THEN ''[]'' ELSE json_agg(${foreignTableAlias}.*) END AS ${foreignTableAlias}`
      );
      relFields.push(foreignTableAlias);
      const joinTableName = foreignTableName;
      const joinTableAlias = foreignTableAlias;
      const [joinTableRelationships, _] = this.relationshipData.get(joinTableName);
      // If this table only has one relationship, it cannot, by definition, be a join table
      if (joinTableRelationships.size == 1) {
        return;
      }
      // Many-to-many relationships (resolves my implicit references via an intermediary; a -> c via a <- b -> c)
      joinTableRelationships.forEach((foreignTableData, joinTableColumnName) => {
        const [foreignTableName, foreignTableColumnName] = foreignTableData;
        if (foreignTableName !== leftTableName) {
          const rightTableName = foreignTableName;
          const rightTableColumnName = foreignTableColumnName;
          const rightTableAlias = `${toSnakeCase(
            joinTableColumnName.split('.').map((word, i, arr) => {
              return arr.length - 1 === i ? plural(word.replace(/[^A-Za-z0-9]/g, '')) : word.replace(/[^A-Za-z0-9]/g, '');
            })
          )}_via_${joinTableAlias}`;
          sqlJoins.push(
            `LEFT JOIN ${rightTableName} AS ${rightTableAlias} ON ${rightTableAlias}.${rightTableColumnName}=${joinTableAlias}.${joinTableColumnName}`
          );
          sqlSelects.push(
            `CASE WHEN COUNT(${rightTableAlias}.*) = 0 THEN ''[]'' ELSE json_agg(${rightTableAlias}.*) END AS ${rightTableAlias}`
          );
          relFields.push(rightTableAlias);
        }
      });
    });

    let sqlSelect = `SELECT ${sqlSelects.join(',')} FROM ${type} ${leftTableAlias} ${sqlJoins.join(' ')}`;

    const reqExpression = new Array<Expression>(
      set(ref('filter'), ref('context.args.filter')),
      set(ref('nextToken'), ref('context.args.nextToken')),
      set(
        ref('limit'),
        // Add 1 to the limit to use for `nextToken
        ref(`util.defaultIfNull($context.args.limit, ${defaultPageLimit}) + 1`)
      ),
      set(
        ref('sortDirections'),
        ref(
          `util.defaultIfNull($context.args.sortDirections, [${defaultSortDirections
            .map(sortDirection => `"${sortDirection}"`)
            .join(',')}])`
        )
      ),
      set(
        ref('tokenFields'),
        ref(`util.defaultIfNull($context.args.tokenFields, [${defaultTokenFields.map(tokenField => `"${tokenField}"`).join(',')}])`)
      ),
      set(
        ref('tokenFieldTypes'),
        ref(
          `util.defaultIfNull($context.args.tokenFieldTypes, [${defaultTokenFieldTypes
            .map(tokenFieldType => `"${tokenFieldType}"`)
            .join(',')}])`
        )
      ),
      // Build SELECT clause
      set(ref('sqlStatement'), raw(`'${sqlSelect}'`)),
      // Build WHERE clause
      iff(raw('!$util.isNullOrEmpty($nextToken) || !$util.isNullOrEmpty($filter)'), set(ref('sqlStatement'), str('$sqlStatement WHERE'))),
      // If we passed in nextToken, we are paging
      iff(
        raw('!$util.isNullOrEmpty($nextToken) && !$util.isNullOrEmpty($tokenFields) && !$util.isNullOrEmpty($tokenFieldTypes)'),
        forEach(ref('tokenField'), ref('tokenFields'), [
          set(ref('idx'), ref('foreach.count - 1')),
          ifElse(
            equals(ref('tokenFieldTypes[$idx]'), str('Int')),
            ifElse(
              equals(ref('sortDirections[$idx]'), str('ASC')),
              set(ref('sqlStatement'), str(`$sqlStatement ${leftTableAlias}.$tokenField >= $nextToken.get($tokenField)`)),
              set(ref('sqlStatement'), str(`$sqlStatement ${leftTableAlias}.$tokenField <= $nextToken.get($tokenField)`))
            ),
            ifElse(
              equals(ref('sortDirections[$idx]'), str('ASC')),
              set(ref('sqlStatement'), str(`$sqlStatement ${leftTableAlias}.$tokenField >= '$nextToken.get($tokenField)'`)),
              set(ref('sqlStatement'), str(`$sqlStatement ${leftTableAlias}.$tokenField <= '$nextToken.get($tokenField)'`))
            )
          ),
          iff(ref('foreach.hasNext'), set(ref('sqlStatement'), str('$sqlStatement AND'))),
        ])
      ),
      // If we passed in a filter, we are filtering (possibly in addition to paging)
      iff(
        raw('!$util.isNullOrEmpty($filter)'),
        ifElse(
          ref('util.isNullOrEmpty($nextToken)'),
          set(ref('sqlStatement'), str('$sqlStatement $filter')),
          set(ref('sqlStatement'), str('$sqlStatement AND $filter'))
        )
      ),
      // Build GROUP BY
      set(ref('sqlStatement'), str('$sqlStatement GROUP BY')),
      forEach(ref('tokenField'), ref('tokenFields'), [
        set(ref('idx'), ref('foreach.count - 1')),
        set(ref('sqlStatement'), str(`$sqlStatement ${leftTableAlias}.$tokenField`)),
        iff(ref('foreach.hasNext'), set(ref('sqlStatement'), str('$sqlStatement,'))),
      ]),
      // Build ORDER BY
      set(ref('sqlStatement'), str('$sqlStatement ORDER BY')),
      forEach(ref('tokenField'), ref('tokenFields'), [
        set(ref('idx'), ref('foreach.count - 1')),
        set(ref('sqlStatement'), str(`$sqlStatement ${leftTableAlias}.$tokenField $sortDirections[$idx]`)),
        iff(ref('foreach.hasNext'), set(ref('sqlStatement'), str('$sqlStatement,'))),
      ]),
      // Build LIMIT - We always want one more than requested
      set(ref('sqlStatement'), str('$sqlStatement LIMIT $limit')),
      // Build query payload
      RelationalDBMappingTemplate.rdsQuery({
        statements: list([str('$sqlStatement')]),
      })
    );

    const resExpression = new Array<Expression>(
      set(ref('result'), ref('utils.rds.toJsonObject($ctx.result)[0]')),
      set(
        ref('tokenFields'),
        ref(`util.defaultIfNull($context.args.tokenFields, [${defaultTokenFields.map(tokenField => `"${tokenField}"`).join(',')}])`)
      ),
      set(ref('items'), raw('[]')),
      set(ref('nextToken'), raw('{}')),
      set(ref('relFields'), raw(`[${relFields.map(relField => `"${relField}"`).join(',')}]`))
    ).concat([
      forEach(ref('item'), ref('result'), [
        forEach(ref('relField'), ref('relFields'), [set(ref(`item[$relField]`), ref(`util.parseJson($item[$relField])`))]),
        ifElse(
          ref('foreach.hasNext'),
          qref('$items.add($item)'),
          ifElse(
            raw('$foreach.count > $ctx.args.limit'),
            forEach(ref('tokenField'), ref('tokenFields'), [qref('$nextToken.put($tokenField, $item.get($tokenField))')]),
            qref('$items.add($item)')
          )
        ),
      ]),
      obj({
        limit: ref('ctx.args.limit'),
        items: ref('util.toJson($items)'),
        nextToken: ref('util.toJson($nextToken)'),
      }),
    ]);

    const reqTemplate = print(compoundExpression(reqExpression));
    const resTemplate = print(compoundExpression(resExpression));

    fs.writeFileSync(`${this.resolverFilePath}/${reqFileName}`, reqTemplate, 'utf8');
    fs.writeFileSync(`${this.resolverFilePath}/${resFileName}`, resTemplate, 'utf8');

    let resolver = new AppSync.Resolver({
      ApiId: Fn.Ref(ResourceConstants.PARAMETERS.AppSyncApiId),
      DataSourceName: Fn.GetAtt(ResourceConstants.RESOURCES.RelationalDatabaseDataSource, 'Name'),
      TypeName: queryTypeName,
      FieldName: fieldName,
      RequestMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: reqFileName,
      }),
      ResponseMappingTemplateS3Location: Fn.Sub(s3BaseUrl, {
        [ResourceConstants.PARAMETERS.S3DeploymentBucket]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentBucket),
        [ResourceConstants.PARAMETERS.S3DeploymentRootKey]: Fn.Ref(ResourceConstants.PARAMETERS.S3DeploymentRootKey),
        [resolverFileName]: resFileName,
      }),
    }).dependsOn([ResourceConstants.RESOURCES.RelationalDatabaseDataSource]);

    return resolver;
  }
}
