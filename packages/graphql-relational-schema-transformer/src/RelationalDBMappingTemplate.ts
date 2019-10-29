import { obj, str, ObjectNode, ListNode, ReferenceNode } from 'graphql-mapping-template';

/**
 * The class that contains the resolver templates for interacting
 * with the Relational Database data source.
 */
export class RelationalDBMappingTemplate {
  /**
   * Provided a SQL statement, creates the rds-query item resolver template.
   *
   * @param param0 - the SQL statement(s) to use when querying the RDS cluster
   * @param param1 - the variable map to use inject values into the SQL statement(s)
   */
  public static rdsQuery({ statements }: { statements: ListNode }): ObjectNode {
    return obj({
      version: str('2018-05-29'),
      statements: statements,
    });
  }
}
