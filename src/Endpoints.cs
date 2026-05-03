using System.Buffers;
using System.Diagnostics;
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
        var detector = app.Services.GetRequiredService<FraudDetector>();

        app.Run(async (ctx) =>
        {
            var path = ctx.Request.Path;

            if (ctx.Request.Method == "GET" && path == "/ready")
            {
                ctx.Response.StatusCode = 200;
                return;
            }

            if (ctx.Request.Method == "POST" && path == "/fraud-score")
            {
                long tStart = Stopwatch.GetTimestamp();

                var result = await ctx.Request.BodyReader.ReadAsync();
                var buffer = result.Buffer;
                FraudData data;
                if (buffer.IsSingleSegment)
                {
                    data = FraudParser.Parse(buffer.FirstSpan);
                }
                else
                {
                    int len = (int)buffer.Length;
                    byte[] rented = ArrayPool<byte>.Shared.Rent(len);
                    buffer.CopyTo(rented);
                    data = FraudParser.Parse(rented.AsSpan(0, len));
                    ArrayPool<byte>.Shared.Return(rented);
                }
                ctx.Request.BodyReader.AdvanceTo(buffer.End);
                long tAfterDeserialize = Stopwatch.GetTimestamp();

                var fraudCount = detector.FraudCount(in data);
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
                return;
            }

            ctx.Response.StatusCode = 404;
        });
    }
}
