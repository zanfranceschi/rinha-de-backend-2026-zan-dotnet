using System.Buffers;
using Rinha2026.Services;

namespace Rinha2026.Endpoints;

public static class Endpoints
{
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

                var fraudCount = detector.FraudCount(in data);

                var payload = FraudDetector.PrecomputedResponses[fraudCount];
                ctx.Response.StatusCode = 200;
                ctx.Response.ContentType = "application/json";
                ctx.Response.ContentLength = payload.Length;
                await ctx.Response.Body.WriteAsync(payload);
                return;
            }

            ctx.Response.StatusCode = 404;
        });
    }
}
