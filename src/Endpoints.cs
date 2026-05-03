using System.Diagnostics;
using Rinha2026.Models;
using Rinha2026.Services;

namespace Rinha2026.Endpoints;

public static class Endpoints
{
    private static long _sRequestTicks;
    private static long _sDeserializeTicks;
    private static long _sFraudCountTicks;
    private static long _sResponseTicks;
    private static long _sRequestCount;

    public static void Map(WebApplication app)
    {
        app.MapGet("/ready", () => Results.Ok());

        app.MapPost("/fraud-score", async (FraudRequest req, FraudDetector detector, HttpContext ctx) =>
        {
            long tStart = Stopwatch.GetTimestamp();

            // req already deserialized by model binding — measure from here
            long tAfterDeserialize = Stopwatch.GetTimestamp();

            var fraudCount = detector.FraudCount(req);
            long tAfterFraud = Stopwatch.GetTimestamp();

            var payload = FraudDetector.PrecomputedResponses[fraudCount];
            ctx.Response.StatusCode = 200;
            ctx.Response.ContentType = "application/json";
            ctx.Response.ContentLength = payload.Length;
            await ctx.Response.Body.WriteAsync(payload);
            long tEnd = Stopwatch.GetTimestamp();

            if (detector.Instrumented)
            {
                Interlocked.Add(ref _sDeserializeTicks, tAfterDeserialize - tStart);
                Interlocked.Add(ref _sFraudCountTicks, tAfterFraud - tAfterDeserialize);
                Interlocked.Add(ref _sResponseTicks, tEnd - tAfterFraud);
                Interlocked.Add(ref _sRequestTicks, tEnd - tStart);
                var n = Interlocked.Increment(ref _sRequestCount);
                if (n % 100 == 0)
                {
                    double freq = Stopwatch.Frequency;
                    double avgReq = Interlocked.Read(ref _sRequestTicks) / n / freq * 1_000_000;
                    double avgDeser = Interlocked.Read(ref _sDeserializeTicks) / n / freq * 1_000_000;
                    double avgFraud = Interlocked.Read(ref _sFraudCountTicks) / n / freq * 1_000_000;
                    double avgResp = Interlocked.Read(ref _sResponseTicks) / n / freq * 1_000_000;
                    Console.WriteLine($"[REQ n={n}] total={avgReq:F0}us deser={avgDeser:F0}us fraud={avgFraud:F0}us resp={avgResp:F0}us");
                }
            }
        });
    }
}
