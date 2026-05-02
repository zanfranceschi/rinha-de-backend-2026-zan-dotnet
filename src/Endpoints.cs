using System.Diagnostics;
using Rinha2026.Models;
using Rinha2026.Services;

namespace Rinha2026.Endpoints;

public static class Endpoints
{
    private static long _count;
    private static long _ticksVectorize;
    private static long _ticksSearch;
    private static long _ticksResponse;

    public static void Map(WebApplication app)
    {
        app.MapGet("/ready", () => Results.Ok());

        app.MapPost("/fraud-score", async (FraudRequest req, FraudDetector detector, HttpContext ctx) =>
        {
            var sw = Stopwatch.StartNew();

            Span<short> query = stackalloc short[DataLoader.Stride];
            Vectorizer.Vectorize(req, detector.Data.Normalization, detector.Data.MccRisk, query);
            var t1 = sw.ElapsedTicks;

            var fraudCount = detector.SearchKnn(query);
            var t2 = sw.ElapsedTicks;

            var payload = FraudDetector.PrecomputedResponses[fraudCount];
            ctx.Response.StatusCode = 200;
            ctx.Response.ContentType = "application/json";
            ctx.Response.ContentLength = payload.Length;
            await ctx.Response.Body.WriteAsync(payload);
            var t3 = sw.ElapsedTicks;

            var n = Interlocked.Increment(ref _count);
            Interlocked.Add(ref _ticksVectorize, t1);
            Interlocked.Add(ref _ticksSearch, t2 - t1);
            Interlocked.Add(ref _ticksResponse, t3 - t2);

            if (n % 100 == 0)
            {
                var freq = (double)Stopwatch.Frequency;
                var msVec = Interlocked.Read(ref _ticksVectorize) / freq * 1000;
                var msSrch = Interlocked.Read(ref _ticksSearch) / freq * 1000;
                var msResp = Interlocked.Read(ref _ticksResponse) / freq * 1000;
                Console.WriteLine($"[PERF n={n}] vectorize={msVec:F2}ms search={msSrch:F2}ms response={msResp:F2}ms (acumulado)");
            }
        });
    }
}
