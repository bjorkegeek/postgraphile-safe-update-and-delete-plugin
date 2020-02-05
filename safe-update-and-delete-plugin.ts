import {
  makePluginByCombiningPlugins,
  makeWrapResolversPlugin
} from "graphile-utils";
import { SchemaBuilder } from "graphile-build";
import { GraphQLNonNull } from "graphql";
import * as pgSql2 from "pg-sql2";
import { Connection, QueryConfig } from "pg";

/**
 * The options passed to the makeSafeUpdateAndDeletePlugin function
 */
export interface SafeUpdateAndDeletePluginOptions {
  /**
   * The name of the column holding the timestamp value
   */
  timestampColumn: string;
}

/**
 * Returns a plugin forcing the client to provide the timestamp of the
 * existing row on update or delete mutations.
 */
export function makeSafeUpdateAndDeletePlugin({
  timestampColumn
}: SafeUpdateAndDeletePluginOptions) {
  return makePluginByCombiningPlugins(
    makeAddTimestampFieldPlugin(timestampColumn),
    makeVerifyTimestampPlugin(timestampColumn)
  );
}

// Implementation is below

type PgSql2 = typeof pgSql2;

interface Inflectors {
  [str: string]: (...args: any[]) => any;
}

// Check if we should omit timestamp verification for this table
function omit({ scope }) {
  const table = scope.pgFieldIntrospection || scope.pgIntrospection;
  return !!table.tags.disableSafeUpdateAndDelete;
}

// Produce the plugin for adding the timestamp field to mutation input
function makeAddTimestampFieldPlugin(timestampColumn: string) {
  return function AddTimestampFieldPlugin(builder: SchemaBuilder) {
    builder.hook(
      "GraphQLInputObjectType:fields",
      (fields, build, { fieldWithHooks, scope }) => {
        let mode: "update" | "delete";
        const attributes = scope.pgIntrospection.attributes;
        if (scope.isPgUpdateInputType) {
          mode = "update";
        } else if (scope.isPgDeleteInputType) {
          mode = "delete";
        } else {
          return fields;
        }
        if (omit({ scope })) {
          return fields;
        }
        const timestampAttribute = attributes.find(
          a => a.name === timestampColumn
        );
        if (!timestampAttribute) {
          return fields;
        }
        const DateTimeType = build.getTypeByName("Datetime");
        const inflection = build.inflection as Inflectors;
        const timestampField = inflection.column(timestampAttribute);
        return build.extend(fields, {
          [timestampField]: fieldWithHooks(
            timestampField,
            ({ addDataGenerator }) => {
              return {
                type: new GraphQLNonNull(DateTimeType),
                description: `The "${timestampField}" value of the existing row to be ${mode}d`
              };
            }
          )
        });
      }
    );
  };
}

// Get SQL fragment for primary key matching condition
function getMutationCondition({
  scope: {
    pgFieldConstraint: { keyAttributes: keys }
  },
  build: { pgSql: _sql, gql2pg, inflection },
  input
}): pgSql2.SQLQuery {
  const sql = _sql as PgSql2;
  return sql.fragment`(${sql.join(
    keys.map(
      key =>
        sql.fragment`${sql.identifier(key.name)} = ${gql2pg(
          input[inflection.column(key)],
          key.type,
          key.typeModifier
        )}`
    ),
    ") and ("
  )})`;
}

// Get SQL fragment for Node ID matching condition
function getNodeMutationCondition({
  scope: { pgFieldIntrospection: table },
  build: {
    pgSql: _sql,
    gql2pg,
    nodeIdFieldName,
    pgGetGqlTypeByTypeIdAndModifier,
    getTypeAndIdentifiersFromNodeId
  },
  input
}): pgSql2.SQLQuery {
  const sql = _sql as PgSql2;
  const primaryKeys =
    table.primaryKeyConstraint && table.primaryKeyConstraint.keyAttributes;
  const nodeId = input[nodeIdFieldName];
  const TableType = pgGetGqlTypeByTypeIdAndModifier(table.type.id, null);
  const { Type, identifiers } = getTypeAndIdentifiersFromNodeId(nodeId);
  if (Type !== TableType) {
    throw new Error("Mismatched type");
  }
  if (identifiers.length !== primaryKeys.length) {
    throw new Error("Invalid ID");
  }
  return sql.fragment`(${sql.join(
    primaryKeys.map(
      (key, idx) =>
        sql.fragment`${sql.identifier(key.name)} = ${gql2pg(
          identifiers[idx],
          key.type,
          key.typeModifier
        )}`
    ),
    ") and ("
  )})`;
}

// Get verification query returning one or more rows with a boolean "ok" column
function getVerifyTimestampSqlQuery({
  scope,
  build,
  input,
  timestampAttribute
}): pgSql2.SQLQuery {
  const { gql2pg } = build;
  const inflection = build.inflection as Inflectors;
  const sql = build.pgSql as PgSql2;
  const table = scope.pgFieldIntrospection;
  let condition: pgSql2.SQLQuery;
  if (scope.isPgNodeMutation) {
    condition = getNodeMutationCondition({ scope, build, input });
  } else {
    condition = getMutationCondition({ scope, build, input });
  }
  const lmValue = gql2pg(
    input[inflection.column(timestampAttribute)],
    timestampAttribute.type,
    timestampAttribute.typeModifier
  );
  const sqlColumn = sql.identifier(timestampAttribute.name);
  return sql.query`\
  select ${lmValue} between (${sqlColumn} - '5 microsecond'::interval)
    and (${sqlColumn} + '5 microsecond'::interval) as ok
    from ${sql.identifier(table.namespace.name, table.name)}
  where ${condition}
  for update`;
}

// Verify the timestamp with the value in the database, throwing an error
// if there is a conflict
async function verifyTimestamp({
  scope,
  build,
  input,
  pgClient,
  timestampAttribute
}): Promise<void> {
  const sql = build.pgSql as PgSql2;
  const sqlCheckQuery = getVerifyTimestampSqlQuery({
    scope,
    build,
    input,
    timestampAttribute
  });
  const sqlString = sql.compile(sqlCheckQuery);
  const { rows } = await pgClient.query(sqlString);
  if (!rows.every(row => row.ok)) {
    throw new Error("Mutation blocked due to conflict.");
  }
}

// Check if this is the type of mutation we update
function isRelevantMutation(scope: { [s: string]: boolean }) {
  return (
    scope.isRootMutation &&
    (scope.isPgUpdateMutationField || scope.isPgDeleteMutationField)
  );
}

// Create the plugin that verifies timestamp before resolving
function makeVerifyTimestampPlugin(timestampColumn: string) {
  return makeWrapResolversPlugin(
    (context, build) => {
      if (
        isRelevantMutation(context.scope) &&
        !omit({ scope: context.scope })
      ) {
        return { scope: context.scope, build };
      }
      return null;
    },
    ({ scope, build }: any) => async (
      resolver,
      user,
      { input },
      context,
      _resolveInfo
    ) => {
      const table = scope.pgFieldIntrospection;
      const timestampAttribute = table.attributes.find(
        a => a.name === timestampColumn
      );
      if (timestampAttribute) {
        const pgClient = context.pgClient as Connection;
        await verifyTimestamp({
          scope,
          build,
          input,
          pgClient,
          timestampAttribute
        });
      }
      return (resolver as any)(); // as any due to typing bug
    }
  );
}
