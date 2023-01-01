using System;
using System.Collections.Generic;

namespace BWJ.Core.SqlRepository
{
    public class SqlQueryCacheService : ISqlQueryCacheService
    {
        private readonly Dictionary<string, string> queryTemplates = new Dictionary<string, string>();
        private readonly object lockObj = new object();

        public string GetQuery(Type entityType, string key, string schema, Func<string> queryBuilder)
        {
            var template = GetTemplate(entityType, key, queryBuilder);
            return string.Format(template, schema);
        }

        public string GetQuery(Type entityType, string key, string schema, string schema2, Func<string> queryBuilder)
        {
            var template = GetTemplate(entityType, key, queryBuilder);
            return string.Format(template, schema, schema2);
        }

        private string GetTemplate(Type entityType, string key, Func<string> queryBuilder)
        {
            var templateKey = $"{entityType.Name}.{key}";
            string? template = null;
            lock (lockObj)
            {
                var templateCached = queryTemplates.ContainsKey(templateKey);
                template = templateCached ? queryTemplates[templateKey] : queryBuilder().Trim();

                if (templateCached == false)
                {
                    queryTemplates.Add(templateKey, template);
                }
            }

            return template;
        }
    }
}
