using Rinha2026;
using Rinha2026.Endpoints;
using Rinha2026.Services;

var builder = WebApplication.CreateSlimBuilder(args);
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default));

var resourcesPath = Environment.GetEnvironmentVariable("RESOURCES_PATH")
    ?? Path.Combine(AppContext.BaseDirectory, "..", "resources");

var dataLoader = new DataLoader(resourcesPath);
builder.Services.AddSingleton(dataLoader);
builder.Services.AddSingleton<FraudDetector>();

var app = builder.Build();

Endpoints.Map(app);

app.Run();
