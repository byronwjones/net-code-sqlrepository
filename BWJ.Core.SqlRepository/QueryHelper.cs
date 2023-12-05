using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace BWJ.Core.SqlRepository
{
    public class QueryHelper<TEntity>
        where TEntity : class
    {
        public QueryHelper(string? alias = null, int schemaIndex = 0)
        {
            if (schemaIndex < 0 || schemaIndex > 1)
            {
                throw new IndexOutOfRangeException("Schema index must be either 0 or 1");
            }

            EntityType = typeof(TEntity);
            TableAlias = alias ?? string.Empty;

            var tableName = EntityType.GetCustomAttribute<TableAttribute>()?.Name ?? $"{EntityType.Name}s";
            TableName = NormalizeTableName(tableName);
            _Table = "[{" + schemaIndex + "}].[" + TableName + "]";
        }

        public QueryHelper(string tableName, string? alias, int schemaIndex = 0)
        {
            if (schemaIndex < 0 || schemaIndex > 1)
            {
                throw new IndexOutOfRangeException("Schema index must be either 0 or 1");
            }

            EntityType = typeof(TEntity);
            TableAlias = alias ?? string.Empty;

            TableName = NormalizeTableName(tableName);
            _Table = "[{" + schemaIndex + "}].[" + TableName + "]";
        }

        public string TableName { get; private set; }

        public string Table(bool includeAlias = true)
            => includeAlias && !string.IsNullOrWhiteSpace(TableAlias) ? $"{_Table} {TableAlias}" : _Table;

        private string _Table;

        public string TableAlias { get; }

        public string StarSelect
        {
            get => string.IsNullOrWhiteSpace(TableAlias) == false ? $"[{TableAlias}].*" : $"*";
        }

        public PropertyInfo? PrimaryKey
        {
            get
            {
                if (SearchedForPrimaryKey == false)
                {
                    _PrimaryKey = DatabaseColumns.FirstOrDefault(p => p.IsKey());
                    if (_PrimaryKey is null)
                    {
                        _PrimaryKey = DatabaseColumns
                            .FirstOrDefault(p => p.Name.Equals("id", StringComparison.OrdinalIgnoreCase));
                    }

                    SearchedForPrimaryKey = true;
                }

                return _PrimaryKey;
            }
        }
        private PropertyInfo? _PrimaryKey;
        private bool SearchedForPrimaryKey = false;

        public List<PropertyInfo> DatabaseColumns
        {
            get
            {
                if (_DatabaseColumns is null)
                {
                    _DatabaseColumns = EntityType.GetProperties()
                        .Where(p => p.IsDatabaseColumn())
                        .ToList();
                }

                return _DatabaseColumns;
            }
        }
        private List<PropertyInfo>? _DatabaseColumns;

        public string ColumnEqualsParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.Equal, columnAndParameterProperty);

        public string ColumnNotEqualsParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.NotEqual, columnAndParameterProperty);

        public string ColumnGreaterThanParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.GreaterThan, columnAndParameterProperty);

        public string ColumnGreaterOrEqualsParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.GreaterThanOrEqual, columnAndParameterProperty);

        public string ColumnLessOrEqualsParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.LessThanOrEqual, columnAndParameterProperty);

        public string ColumnLessThanParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.LessThan, columnAndParameterProperty);

        public string ColumnInParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.In, columnAndParameterProperty);

        public string ColumnNotInParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.NotIn, columnAndParameterProperty);

        public string ColumnLikeParameter<TProperty>(Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            CompareColumnToParameter(SqlOperator.Like, columnAndParameterProperty);

        /// <summary>
        /// Generates an SQL script snippet where a column is compared to a parameter of the same name
        /// </summary>
        /// <param name="op">Comparison operator</param>
        public string CompareColumnToParameter<TProperty>(SqlOperator op, Expression<Func<TEntity, TProperty>> columnAndParameterProperty) =>
            $"{Column(columnAndParameterProperty)} {op.ToOperator()} {Parameter(columnAndParameterProperty)}";

        public string Column<TProperty>(Expression<Func<TEntity, TProperty>> columnProperty)
        {
            var col = GetPropertyFromLambda(columnProperty);
            return Column(col);
        }

        public string InnerJoin<TProperty, TJoinTable>(
            QueryHelper<TJoinTable> joinTable,
            Expression<Func<TEntity, TProperty>> column,
            Expression<Func<TJoinTable, TProperty>> joinColumn,
            SqlOperator op = SqlOperator.Equal)
            where TJoinTable : class
            => ScriptJoin("INNER", joinTable, column, joinColumn, op);

        public string LeftOuterJoin<TProperty, TJoinTable>(
            QueryHelper<TJoinTable> joinTable,
            Expression<Func<TEntity, TProperty>> column,
            Expression<Func<TJoinTable, TProperty>> joinColumn,
            SqlOperator op = SqlOperator.Equal)
            where TJoinTable : class
            => ScriptJoin("LEFT OUTER", joinTable, column, joinColumn, op);

        private string ScriptJoin<TProperty, TJoinTable>(
            string joinType,
            QueryHelper<TJoinTable> joinTable,
            Expression<Func<TEntity, TProperty>> column,
            Expression<Func<TJoinTable, TProperty>> joinColumn,
            SqlOperator op = SqlOperator.Equal)
            where TJoinTable : class
            => $@"{joinType} JOIN {joinTable.Table()}
                    ON {Column(column)} {op.ToOperator()} {joinTable.Column(joinColumn)}";

        public string SelectAllExcept(params Expression<Func<TEntity, object?>>[] excludeColumns)
        {
            var columns = DatabaseColumns;
            var remove = new List<PropertyInfo>();
            foreach (var col in excludeColumns)
            {
                var prop = columns.FirstOrDefault(p => p.Name == col.Name);
                if (prop is not null) { remove.Add(prop); }
            }
            remove.ForEach(col => columns.Remove(col));

            return string.Join(", ", columns.Select(c => Column(c)));
        }

        public string Column(PropertyInfo columnProperty)
        {
            var col = GetDatabaseColumnProperty(columnProperty)
                .ToColumnString();

            return string.IsNullOrWhiteSpace(TableAlias) ? col : $"[{TableAlias}].{col}";
        }

        public string Parameter<TProperty>(Expression<Func<TEntity, TProperty>> parameterProperty)
        {
            return GetDatabaseColumnProperty(parameterProperty)
                .ToParameterString();
        }

        public string Parameter(PropertyInfo parameterProperty)
        {
            return GetDatabaseColumnProperty(parameterProperty)
                .ToParameterString();
        }

        public static string FormatValueForBeginsWithQuery(string searchTerm)
            => string.IsNullOrEmpty(searchTerm) == false ?
            $"{EscapeForLikeQuery(searchTerm)}%" : string.Empty;

        public static string FormatValueForEndsWithQuery(string searchTerm)
            => string.IsNullOrEmpty(searchTerm) == false ?
            $"%{EscapeForLikeQuery(searchTerm)}" : string.Empty;

        public static string FormatValueForContainsTextQuery(string searchTerm)
            => string.IsNullOrEmpty(searchTerm) == false ?
            $"%{EscapeForLikeQuery(searchTerm)}%" : string.Empty;

        public static string? EscapeForLikeQuery(string? searchTerm)
            => searchTerm?
            .Replace("_", "[_]")
            .Replace("%", "[%]")
            .Replace("[", "[[]");

        /// <summary>
        /// Remove client table prefix from client table names
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private string NormalizeTableName(string tableName)
        {
            tableName = tableName?.Trim() ?? string.Empty;
            return Regex.Replace(tableName, @"^Xlient_*", string.Empty, RegexOptions.IgnoreCase);
        }

        private PropertyInfo GetDatabaseColumnProperty<TProperty>(Expression<Func<TEntity, TProperty>> columnProperty)
        {
            var prop = GetPropertyFromLambda(columnProperty);
            return GetDatabaseColumnProperty(prop);
        }

        private PropertyInfo GetDatabaseColumnProperty(PropertyInfo columnProperty)
        {
            if (columnProperty.IsDatabaseColumn() == false)
            {
                throw new ArgumentException("Property is not a database column");
            }

            return columnProperty;
        }

        // see https://stackoverflow.com/questions/671968/retrieving-property-name-from-lambda-expression
        private PropertyInfo GetPropertyFromLambda<TProperty>(Expression<Func<TEntity, TProperty>> propertyLambda)
        {
            Type type = typeof(TEntity);

            MemberExpression? member = propertyLambda.Body as MemberExpression;
            if (member == null)
            {
                throw new ArgumentException($"Expression '{propertyLambda}' refers to a method, not a property.");
            }

            PropertyInfo? property = member.Member as PropertyInfo;
            if (property == null)
            {
                throw new ArgumentException($"Expression '{propertyLambda}' refers to a field, not a property.");
            }

            return property;
        }

        private readonly Type EntityType;
    }
}
