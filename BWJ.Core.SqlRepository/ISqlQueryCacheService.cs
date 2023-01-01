using System;

namespace BWJ.Core.SqlRepository
{
    public interface ISqlQueryCacheService
    {
        string GetQuery(Type entityType, string key, string schema, Func<string> queryBuilder);
    }
}