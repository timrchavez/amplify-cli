import {
  Kind,
  ObjectTypeDefinitionNode,
  NonNullTypeNode,
  DirectiveNode,
  NameNode,
  OperationTypeNode,
  FieldDefinitionNode,
  NamedTypeNode,
  InputValueDefinitionNode,
  ValueNode,
  OperationTypeDefinitionNode,
  ArgumentNode,
  ListValueNode,
  ListTypeNode,
  StringValueNode,
  InputObjectTypeDefinitionNode,
} from 'graphql';

/**
 * Creates a non-null type, which is a node wrapped around another type that simply defines it is non-nullable.
 *
 * @param typeNode the type to be marked as non-nullable.
 * @returns a non-null wrapper around the provided type.
 */
export function getNonNullType(typeNode: NamedTypeNode | ListTypeNode): NonNullTypeNode {
  return {
    kind: Kind.NON_NULL_TYPE,
    type: typeNode,
  };
}

/**
 * Creates a named type for the schema.
 *
 * @param name the name of the type.
 * @returns a named type with the provided name.
 */
export function getNamedType(name: string): NamedTypeNode {
  return {
    kind: Kind.NAMED_TYPE,
    name: {
      kind: Kind.NAME,
      value: name,
    },
  };
}

/**
 * Creates a list type for the schema.
 *
 * @param name the name of the type.
 * @returns a named type with the provided name.
 */
export function getListType(name: string): ListTypeNode {
  return {
    kind: Kind.LIST_TYPE,
    type: getNamedType(name),
  };
}

/**
 * Creates an input value definition for the schema.
 *
 * @param typeNode the type of the input node.
 * @param name the name of the input.
 * @returns an input value definition node with the provided type and name.
 */
export function getInputValueDefinition(typeNode: NamedTypeNode | NonNullTypeNode | ListTypeNode, name: string): InputValueDefinitionNode {
  return {
    kind: Kind.INPUT_VALUE_DEFINITION,
    name: {
      kind: Kind.NAME,
      value: name,
    },
    type: typeNode,
  };
}

/**
 * Creates an operation field definition for the schema.
 *
 * @param name the name of the operation.
 * @param args the arguments for the operation.
 * @param type the type of the operation.
 * @param directives the directives (if any) applied to this field. In this context, only subscriptions will have this.
 * @returns an operation field definition with the provided name, args, type, and optionally directives.
 */
export function getOperationFieldDefinition(
  name: string,
  args: InputValueDefinitionNode[],
  type: NamedTypeNode,
  directives: ReadonlyArray<DirectiveNode>
): FieldDefinitionNode {
  return {
    kind: Kind.FIELD_DEFINITION,
    name: {
      kind: Kind.NAME,
      value: name,
    },
    arguments: args,
    type: type,
    directives: directives,
  };
}

/**
 * Creates a field definition node for the schema.
 *
 * @param fieldName the name of the field to be created.
 * @param type the type of the field to be created.
 * @returns a field definition node with the provided name and type.
 */
export function getFieldDefinition(fieldName: string, type: NonNullTypeNode | NamedTypeNode | ListTypeNode): FieldDefinitionNode {
  return {
    kind: Kind.FIELD_DEFINITION,
    name: {
      kind: Kind.NAME,
      value: fieldName,
    },
    type,
  };
}

/**
 * Creates a type definition node for the schema.
 *
 * @param fields the field set to be included in the type.
 * @param typeName the name of the type.
 * @returns a type definition node defined by the provided fields and name.
 */
export function getTypeDefinition(fields: ReadonlyArray<FieldDefinitionNode>, typeName: string): ObjectTypeDefinitionNode {
  return {
    kind: Kind.OBJECT_TYPE_DEFINITION,
    name: {
      kind: Kind.NAME,
      value: typeName,
    },
    fields: fields,
  };
}

/**
 * Creates an input type definition node for the schema.
 *
 * @param fields the fields in the input type.
 * @param typeName the name of the input type
 * @returns an input type definition node defined by the provided fields and
 */
export function getInputTypeDefinition(fields: ReadonlyArray<InputValueDefinitionNode>, typeName: string): InputObjectTypeDefinitionNode {
  return {
    kind: Kind.INPUT_OBJECT_TYPE_DEFINITION,
    name: {
      kind: Kind.NAME,
      value: typeName,
    },
    fields: fields,
  };
}

/**
 * Creates a name node for the schema.
 *
 * @param name the name of the name node.
 * @returns the name node defined by the provided name.
 */
export function getNameNode(name: string): NameNode {
  return {
    kind: Kind.NAME,
    value: name,
  };
}

/**
 * Creates a list value node for the schema.
 *
 * @param values the list of values to be in the list node.
 * @returns a list value node containing the provided values.
 */
export function getListValueNode(values: ReadonlyArray<ValueNode>): ListValueNode {
  return {
    kind: Kind.LIST,
    values: values,
  };
}

/**
 * Creates a simple string value node for the schema.
 *
 * @param value the value to be set in the string value node.
 * @returns a fleshed-out string value node.
 */
export function getStringValueNode(value: string): StringValueNode {
  return {
    kind: Kind.STRING,
    value: value,
  };
}

/**
 * Creates a directive node for a subscription in the schema.
 *
 * @param mutationName the name of the mutation the subscription directive is for.
 * @returns a directive node defining the subscription.
 */
export function getDirectiveNode(mutationName: string): DirectiveNode {
  return {
    kind: Kind.DIRECTIVE,
    name: this.getNameNode('aws_subscribe'),
    arguments: [this.getArgumentNode(mutationName)],
  };
}

/**
 * Creates an operation type definition (subscription, query, mutation) for the schema.
 *
 * @param operationType the type node defining the operation type.
 * @param operation  the named type node defining the operation type.
 */
export function getOperationTypeDefinition(operationType: OperationTypeNode, operation: NamedTypeNode): OperationTypeDefinitionNode {
  return {
    kind: Kind.OPERATION_TYPE_DEFINITION,
    operation: operationType,
    type: operation,
  };
}

/**
 * Creates an argument node for a subscription directive within the schema.
 *
 * @param argument the argument string.
 * @returns the argument node.
 */
export function getArgumentNode(argument: string): ArgumentNode {
  return {
    kind: Kind.ARGUMENT,
    name: this.getNameNode('mutations'),
    value: this.getListValueNode([this.getStringValueNode(argument)]),
  };
}

/**
 * Creates a GraphQL connection type for a given GraphQL type, corresponding to a SQL table name.
 * Accounts for the possibility that `nextToken` is a composite key.
 *
 * @param name the name of the SQL table (and GraphQL type).
 * @returns a type definition node defining the connection type for the provided type name.
 */
export function getConnectionTypeDefinition(name: string): ObjectTypeDefinitionNode {
  return getTypeDefinition(
    [
      getFieldDefinition('items', getNamedType(`[${name}]`)),
      getFieldDefinition('limit', getNamedType('Int')),
      getFieldDefinition('nextToken', getNamedType(`${name}Keys`)),
    ],
    `${name}Connection`
  );
}

/**
 * Creates a GraphQL primary key type for a given GraphQL type, corresponding to a SQL table name.
 * Accounts for the possibility of composite primary key.
 *
 * @param name the name of the SQL table (and GraphQL type).
 * @param fieldNames the list of primary key column names
 * @param fieldTypes the list of primary key field types
 * @returns a type definition node defining the primary type for the provided type name
 */
export function getKeysTypeDefinition(name: string, fieldNames: string[], fieldTypes: string[]) {
  let fields = [];
  fieldNames.forEach((fieldName, pos) => {
    fields.push(getFieldDefinition(fieldName, getNamedType(fieldTypes[pos])));
  });
  return getTypeDefinition(fields, `${name}Keys`);
}
