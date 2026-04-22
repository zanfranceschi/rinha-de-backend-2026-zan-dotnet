using System.Text.Json.Serialization;
using Rinha2026.Models;

namespace Rinha2026;

[JsonSerializable(typeof(FraudRequest))]
[JsonSerializable(typeof(FraudResponse))]
[JsonSerializable(typeof(Dictionary<string, double>))]
[JsonSerializable(typeof(NormalizationConfig))]
[JsonSerializable(typeof(List<Reference>))]
internal partial class AppJsonContext : JsonSerializerContext { }
