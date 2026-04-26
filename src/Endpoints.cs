using Rinha2026.Models;
using Rinha2026.Services;

namespace Rinha2026.Endpoints;

public static class Endpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/ready", () => Results.Ok());

        app.MapPost("/fraud-score", async (FraudRequest req, FraudDetector detector, HttpContext ctx) =>
        {
            var fraudCount = detector.FraudCount(req);
            var payload = FraudDetector.PrecomputedResponses[fraudCount];

            ctx.Response.StatusCode = 200;
            ctx.Response.ContentType = "application/json";
            ctx.Response.ContentLength = payload.Length;
            await ctx.Response.Body.WriteAsync(payload);
        });
    }
}
