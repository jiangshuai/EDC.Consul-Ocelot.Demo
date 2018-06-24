using AspectCore.DynamicProxy;
using Microsoft.Extensions.Caching.Memory;
using Polly;
using System;
using System.Threading.Tasks;

namespace Manulife.DNC.MSAD.Common
{
    [AttributeUsage(AttributeTargets.Method)]
    public class HystrixCommandAttribute : AbstractInterceptorAttribute
    {
        /// <summary>
        /// 最多重试几次：如果为0，则不重试
        /// </summary>
        public int MaxRetryTimes { get; set; }

        /// <summary>
        /// 重试间隔（单位：毫秒）：默认100ms
        /// </summary>
        public int RetryIntervalMilliseconds { get; set; } = 100;

        /// <summary>
        /// 是否启用熔断
        /// </summary>
        public bool IsEnableCircuitBreaker { get; set; } = false;

        /// <summary>
        /// 熔断前出现允许错误几次
        /// </summary>
        public int ExceptionsAllowedBeforeBreaking { get; set; } = 3;

        /// <summary>
        /// 熔断时间（单位：毫秒）：默认1000ms
        /// </summary>
        public int MillisecondsOfBreak { get; set; } = 1000;

        /// <summary>
        /// 执行超过多少毫秒则认为超时（0表示不检测超时）
        /// </summary>
        public int TimeOutMilliseconds { get; set; } = 0;

        /// <summary>
        /// 缓存时间（存活期，单位：毫秒）：默认为0，表示不缓存
        /// Key：类名+方法名+所有参数ToString
        /// </summary>
        public int CacheTTLMilliseconds { get; set; } = 0;

        private Policy policy;
        private static readonly IMemoryCache memoryCache = new MemoryCache(new MemoryCacheOptions());

        /// <summary>
        /// 降级的方法名称
        /// </summary>
        public string FallBackMethod { get; set; }

        public HystrixCommandAttribute(string fallBackMethod)
        {
            FallBackMethod = fallBackMethod;
        }

        public override async Task Invoke(AspectContext context, AspectDelegate next)
        {
            // Polly CircuitBreaker要求对于同一段代码要共享一个policy对象
            lock (this) // 线程安全考虑
            {
                if (policy == null)
                {
                    policy = Policy.Handle<Exception>().FallbackAsync(async (ctx, t) =>
                    {
                        AspectContext aspectContext = (AspectContext)ctx["aspectContext"];
                        var fallBackMethod = context.ServiceMethod.DeclaringType.GetMethod(FallBackMethod);
                        var fallBackResult = fallBackMethod.Invoke(context.Implementation, context.Parameters);
                        aspectContext.ReturnValue = fallBackResult;
                    }, async (ex, t) => { });
                }

                // 设置 最大重试次数限制
                if (MaxRetryTimes > 0)
                {
                    policy = policy.WrapAsync(Policy.Handle<Exception>()
                        .WaitAndRetryAsync(MaxRetryTimes,
                        i => TimeSpan.FromMilliseconds(RetryIntervalMilliseconds)));
                }

                // 启用熔断保护（CircuitBreaker）
                if (IsEnableCircuitBreaker)
                {
                    policy = policy.WrapAsync(Policy.Handle<Exception>()
                        .CircuitBreakerAsync(ExceptionsAllowedBeforeBreaking,
                        TimeSpan.FromMilliseconds(MillisecondsOfBreak), (ex, ts) =>
                        {
                                // assuem to do logging
                                Console.WriteLine($"Service API OnBreak -- ts = {ts.Seconds}s, ex.message = {ex.Message}");
                        }, () => 
                        {
                            // assume to do logging
                            Console.WriteLine($"Service API OnReset");
                        }));
                }

                // 设置超时时间
                if (TimeOutMilliseconds > 0)
                {
                    policy = policy.WrapAsync(Policy.TimeoutAsync(() =>
                        TimeSpan.FromMilliseconds(TimeOutMilliseconds),
                        Polly.Timeout.TimeoutStrategy.Pessimistic));
                }
            }

            Context pollyContext = new Context();
            pollyContext["aspectContext"] = context;

            // 设置缓存时间
            if (CacheTTLMilliseconds > 0)
            {
                string cacheKey = $"HystrixMethodCacheManager_Key_{context.ServiceMethod.DeclaringType}.{context.ServiceMethod}"
                    + string.Join("_", context.Parameters);

                if (memoryCache.TryGetValue(cacheKey, out var cacheValue))
                {
                    // 如果缓存中有，直接用缓存的值
                    context.ReturnValue = cacheValue;
                }
                else
                {
                    // 如果缓存中没有，则执行实际被拦截的方法
                    await policy.ExecuteAsync(ctx => next(context), pollyContext);
                    // 执行完被拦截方法后存入缓存中以便后面快速复用
                    using (var cacheEntry = memoryCache.CreateEntry(cacheKey))
                    {
                        cacheEntry.Value = context.ReturnValue;
                        cacheEntry.AbsoluteExpiration = DateTime.Now
                            + TimeSpan.FromMilliseconds(CacheTTLMilliseconds); // 设置缓存过期时间
                    }
                }
            }
            else
            {
                // 如果没有启用缓存，则直接执行业务方法
                await policy.ExecuteAsync(ctx => next(context), pollyContext);
            }
        }
    }
}
