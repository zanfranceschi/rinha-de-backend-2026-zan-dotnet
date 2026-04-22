using Rinha2026.Models;
using Rinha2026.Services;

namespace Rinha2026.Endpoints;

public static class Endpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/ready", () => Results.Ok());

        app.MapPost("/fraud-score", (FraudRequest req, FraudDetector detector) =>
            Results.Ok(detector.Evaluate(req)));
    }
}
