using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BWJ.Core.SqlRepository
{
    public abstract class SqlRepositoryBase<TEntity, TIdentifier>
        where TEntity : class
    {
        private readonly string _cs;
        private readonly ISqlQueryCacheService _queryService;

        public SqlRepositoryBase(IConfiguration _config, ISqlQueryCacheService queryService)
        {
            _cs = GetConnectionString(_config);
            _queryService = queryService;
        }

        protected abstract string GetConnectionString(IConfiguration config);

        protected async Task<TEntity?> GetOne(string query, object? parameters = null)
            => (await GetMany(query, parameters)).FirstOrDefault();

        protected async Task<TEntity?> GetOne<TChildEntity>(
            string query,
            Func<TEntity, TChildEntity, TEntity> entityBuilder,
            IEnumerable<string> splitOn,
            object? parameters = null)
            => (await GetMany(query, entityBuilder, splitOn, parameters)).FirstOrDefault();
        protected async Task<TEntity?> GetOne<TChildEntity, TChildEntity2>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TEntity> entityBuilder,
            IEnumerable<string> splitOn,
            object? parameters = null)
            => (await GetMany(query, entityBuilder, splitOn, parameters)).FirstOrDefault();
        protected async Task<TEntity?> GetOne<TChildEntity, TChildEntity2, TChildEntity3>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TChildEntity3, TEntity> entityBuilder,
            IEnumerable<string> splitOn,
            object? parameters = null)
            => (await GetMany(query, entityBuilder, splitOn, parameters)).FirstOrDefault();
        protected async Task<TEntity?> GetOne<TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4, TEntity> entityBuilder,
            string splitOn,
            object? parameters = null)
            => (await GetMany(query, entityBuilder, splitOn, parameters)).FirstOrDefault();
        protected async Task<TEntity?> GetOne<TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4, TChildEntity5>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4, TChildEntity5, TEntity> entityBuilder,
            string splitOn,
            object? parameters = null)
            => (await GetMany(query, entityBuilder, splitOn, parameters)).FirstOrDefault();

        protected async Task<IEnumerable<TEntity>> GetMany(string query, object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync<TEntity>(query, parameters);
            }
        }
        protected async Task<IEnumerable<TEntity>> GetMany<TChildEntity>(
            string query,
            Func<TEntity, TChildEntity, TEntity> entityBuilder,
            IEnumerable<string> splitOn,
            object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync(query, entityBuilder, parameters, splitOn: string.Join(',', splitOn));
            }
        }
        protected async Task<IEnumerable<TEntity>> GetMany<TChildEntity, TChildEntity2>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TEntity> entityBuilder,
            IEnumerable<string> splitOn,
            object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync(query, entityBuilder, parameters, splitOn: string.Join(',', splitOn));
            }
        }
        protected async Task<IEnumerable<TEntity>> GetMany<TChildEntity, TChildEntity2, TChildEntity3>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TChildEntity3, TEntity> entityBuilder,
            IEnumerable<string> splitOn,
            object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync(query, entityBuilder, parameters, splitOn: string.Join(',', splitOn));
            }
        }
        protected async Task<IEnumerable<TEntity>> GetMany<TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4, TEntity> entityBuilder,
            string splitOn,
            object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync(query, entityBuilder, parameters, splitOn: splitOn);
            }
        }
        protected async Task<IEnumerable<TEntity>> GetMany<TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4, TChildEntity5>(
            string query,
            Func<TEntity, TChildEntity, TChildEntity2, TChildEntity3, TChildEntity4, TChildEntity5, TEntity> entityBuilder,
            string splitOn,
            object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync(query, entityBuilder, parameters, splitOn: splitOn);
            }
        }

        protected async Task<T?> GetScalar<T>(string query, object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.ExecuteScalarAsync<T>(query, parameters);
            }
        }

        protected async Task<IEnumerable<T>> GetPrimitiveList<T>(string query, object? parameters = null)
        {
            var listType = typeof(T);
            if (IsDatabasePrimitiveType(listType) == false)
            {
                throw new ArgumentException($"Type {listType.FullName} does not map to a database primitive type");
            }

            using (var conn = getSqlConnection())
            {
                conn.Open();
                return await conn.QueryAsync<T>(query, parameters);
            }
        }

        private bool IsDatabasePrimitiveType(Type? t)
        {
            if (t is null) { return false; }

            return t.IsValueType
                || t == typeof(string)
                || t == typeof(DateTime)
                || t == typeof(decimal)
                || t == typeof(byte[])
                || IsDatabasePrimitiveType(Nullable.GetUnderlyingType(t));
        }

        protected async Task Execute(string query, object? parameters = null)
        {
            using (var conn = getSqlConnection())
            {
                conn.Open();
                await conn.ExecuteAsync(query, parameters);
            }
        }

        protected async Task<TIdentifier> CreateRecord(TEntity record, string schema)
        {
            var x = new QueryHelper<TEntity>();

            var query = GetQuery("CreateRecord", schema, () =>
            {
                var columns = x.DatabaseColumns
                .Where(c => c.IsGeneratedKey() == false)
                .ToList();

                return $@"
                    INSERT INTO {x.Table()}
                               ({string.Join(',', columns.Select(c => c.ToColumnString()))})
                         VALUES
                               ({string.Join(',', columns.Select(c => c.ToParameterString()))});
                    SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY];";
            });

            return (await GetScalar<TIdentifier>(query, record))!;
        }

        protected async Task UpdateRecord(TEntity record, string schema)
        {
            var query = GetQuery("UpdateRecord", schema, () =>
            {
                var x = new QueryHelper<TEntity>();

                if (x.PrimaryKey is null)
                {
                    throw new FormatException($"A property on entity class {typeof(TEntity).Name} must have the name Id or a key attribute");
                }

                var updateExpressions = x.DatabaseColumns.Where(c => c.IsKey() == false)
                .Select(c => $"{c.ToColumnString()} = {c.ToParameterString()}");

                return $@"
                    UPDATE {x.Table()}
                       SET {string.Join(',', updateExpressions)}
                     WHERE {x.PrimaryKey.ToColumnString()} = {x.PrimaryKey.ToParameterString()}";
            });

            await Execute(query, record);
        }

        protected async Task RemoveRecord(TEntity record, string schema)
        {
            var x = new QueryHelper<TEntity>();

            var query = GetQuery("RemoveRecord", schema, () =>
            {
                if (x.PrimaryKey is null)
                {
                    throw new FormatException($"A property on entity class {typeof(TEntity).Name} must have the name Id or a key attribute");
                }

                return $@"
                    DELETE FROM {x.Table()}
                          WHERE {x.PrimaryKey.ToColumnString()} = {x.PrimaryKey.ToParameterString()}";
            });

            await Execute(query, record);
        }

        protected async Task RemoveRecordById(TIdentifier id, string schema)
        {
            var query = GetQuery("RemoveRecordById", schema, () =>
            {
                var x = new QueryHelper<TEntity>();

                if (x.PrimaryKey is null)
                {
                    throw new FormatException($"A property on entity class {typeof(TEntity).Name} must have the name Id or a key attribute");
                }

                return $@"
                    DELETE FROM {x.Table()}
                          WHERE {x.PrimaryKey.ToColumnString()} = @Id";
            });

            await Execute(query, new { Id = id });
        }

        protected async Task<TEntity?> GetRecord(TIdentifier id, string schema)
        {
            var query = GetQuery("GetRecord", schema, () =>
            {
                var x = new QueryHelper<TEntity>();

                if (x.PrimaryKey is null)
                {
                    throw new FormatException($"A property on entity class {typeof(TEntity).Name} must have the name Id or a key attribute");
                }

                return $@"
                    SELECT * FROM {x.Table()}
                          WHERE {x.PrimaryKey.ToColumnString()} = @Id";
            });

            return await GetOne(query, new { Id = id });
        }

        protected async Task<IEnumerable<TEntity>> GetAll()
        {
            using (var conn = getSqlConnection())
            {
                await conn.OpenAsync();
                return await conn.GetAllAsync<TEntity>();
            }
        }

        protected SqlConnection getSqlConnection() => new SqlConnection(_cs);

        protected string GetQuery(string key, string schema, Func<string> queryBuilder) =>
            _queryService.GetQuery(typeof(TEntity), key, schema, queryBuilder);
    }

    public abstract class SqlRepositoryBase<TEntity> : SqlRepositoryBase<TEntity, long>
        where TEntity : class
    {
        public SqlRepositoryBase(IConfiguration _config, ISqlQueryCacheService queryService)
            : base(_config, queryService) { }
    }
}
