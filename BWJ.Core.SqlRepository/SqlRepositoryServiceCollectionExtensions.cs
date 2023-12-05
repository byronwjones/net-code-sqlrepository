using Microsoft.Extensions.DependencyInjection;
using System.Linq;

namespace BWJ.Core.SqlRepository
{
    public static class SqlRepositoryServiceCollectionExtensions
    {
        public static IServiceCollection RegisterDatabaseRepositories<TFromAssembly>(this IServiceCollection services, bool registerAsScoped = false)
        {
            services.AddSingleton<ISqlQueryCacheService, SqlQueryCacheService>();

            var types = typeof(TFromAssembly).Assembly.GetExportedTypes();
            var repos = types.Where(t => t.IsSubclassOfGenericClassDefinition(typeof(SqlRepositoryBase<,>)));

            foreach (var repository in repos)
            {
                var iface = repository.GetInterface($"I{repository.Name}", ignoreCase: true);

                if (iface is not null)
                {
                    if(registerAsScoped)
                    {
                        services.AddScoped(iface, repository);
                    }
                    else
                    {
                        services.AddSingleton(iface, repository);
                    }
                }
            }

            return services;
        }
    }
}
