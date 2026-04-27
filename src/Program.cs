using Rinha2026;
using Rinha2026.Endpoints;
using Rinha2026.Services;

var builder = WebApplication.CreateSlimBuilder(args);
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default));

var socketPath = Environment.GetEnvironmentVariable("SOCKET_PATH");
if (!string.IsNullOrEmpty(socketPath))
{
    if (File.Exists(socketPath)) File.Delete(socketPath);
    builder.WebHost.ConfigureKestrel(k => k.ListenUnixSocket(socketPath));
}

var resourcesPath = Environment.GetEnvironmentVariable("RESOURCES_PATH")
    ?? Path.Combine(AppContext.BaseDirectory, "..", "resources");

var dataLoader = new DataLoader(resourcesPath);
var fraudDetector = new FraudDetector(dataLoader);
builder.Services.AddSingleton(dataLoader);
builder.Services.AddSingleton(fraudDetector);

var app = builder.Build();

Endpoints.Map(app);

if (!string.IsNullOrEmpty(socketPath))
{
    app.Lifetime.ApplicationStarted.Register(() =>
    {
        if (File.Exists(socketPath))
            File.SetUnixFileMode(socketPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute);
    });
}

app.Run();
