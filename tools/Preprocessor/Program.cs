using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization;

var resourcesPath = args.Length > 0
    ? args[0]
    : Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "resources");

var k = args.Length > 1 ? int.Parse(args[1]) : 0;
var maxIterations = args.Length > 2 ? int.Parse(args[2]) : 50;

Console.WriteLine($"Loading references from {resourcesPath}...");
List<Reference> references;
using (var gz = new GZipStream(File.OpenRead(Path.Combine(resourcesPath, "references.json.gz")), CompressionMode.Decompress))
{
    references = JsonSerializer.Deserialize(gz, RefJsonContext.Default.ListReference)!;
}
Console.WriteLine($"Loaded {references.Count} references.");

var fraud = references.Where(r => r.Label == "fraud").Select(r => r.Vector).ToList();
var legit = references.Where(r => r.Label == "legit").Select(r => r.Vector).ToList();
Console.WriteLine($"Fraud: {fraud.Count}, Legit: {legit.Count}");

double[][] fraudCentroids;
double[][] legitCentroids;

if (k > 0)
{
    var fraudRatio = (double)fraud.Count / references.Count;
    var fraudK = Math.Max(1, (int)Math.Round(k * fraudRatio));
    var legitK = k - fraudK;
    Console.WriteLine($"Clustering: {fraudK} fraud centroids + {legitK} legit centroids = {k} total");

    Console.WriteLine("Clustering fraud vectors...");
    fraudCentroids = KMeans(fraud, fraudK, maxIterations);
    Console.WriteLine("Clustering legit vectors...");
    legitCentroids = KMeans(legit, legitK, maxIterations);
}
else
{
    Console.WriteLine("No clustering — converting all references to binary.");
    fraudCentroids = fraud.ToArray();
    legitCentroids = legit.ToArray();
}

// Write binary format: [int32 count][per ref: 14 doubles + 1 byte label (1=fraud, 0=legit)]
var outputPath = Path.Combine(resourcesPath, "references.bin");
using (var bw = new BinaryWriter(File.Create(outputPath)))
{
    bw.Write(fraudCentroids.Length + legitCentroids.Length);
    foreach (var c in fraudCentroids)
    {
        foreach (var d in c) bw.Write(d);
        bw.Write((byte)1);
    }
    foreach (var c in legitCentroids)
    {
        foreach (var d in c) bw.Write(d);
        bw.Write((byte)0);
    }
}
var fileSize = new FileInfo(outputPath).Length;
Console.WriteLine($"Written {fraudCentroids.Length + legitCentroids.Length} references to {outputPath} ({fileSize} bytes)");

static double[][] KMeans(List<double[]> vectors, int k, int maxIterations)
{
    var dims = vectors[0].Length;
    var rng = new Random(42);

    // Initialize centroids via random selection
    var centroids = new double[k][];
    var indices = Enumerable.Range(0, vectors.Count).OrderBy(_ => rng.Next()).Take(k).ToList();
    for (int i = 0; i < k; i++)
        centroids[i] = (double[])vectors[indices[i]].Clone();

    var assignments = new int[vectors.Count];

    for (int iter = 0; iter < maxIterations; iter++)
    {
        // Assign each vector to nearest centroid
        var changed = 0;
        for (int v = 0; v < vectors.Count; v++)
        {
            var nearest = 0;
            var nearestDist = SquaredDistance(vectors[v], centroids[0]);
            for (int c = 1; c < k; c++)
            {
                var dist = SquaredDistance(vectors[v], centroids[c]);
                if (dist < nearestDist)
                {
                    nearestDist = dist;
                    nearest = c;
                }
            }
            if (assignments[v] != nearest)
            {
                assignments[v] = nearest;
                changed++;
            }
        }

        Console.WriteLine($"  Iteration {iter + 1}: {changed} reassignments");
        if (changed == 0)
            break;

        // Recompute centroids
        var sums = new double[k][];
        var counts = new int[k];
        for (int c = 0; c < k; c++)
            sums[c] = new double[dims];

        for (int v = 0; v < vectors.Count; v++)
        {
            var c = assignments[v];
            counts[c]++;
            for (int d = 0; d < dims; d++)
                sums[c][d] += vectors[v][d];
        }

        for (int c = 0; c < k; c++)
        {
            if (counts[c] == 0)
            {
                // Reinitialize empty centroid to a random vector
                centroids[c] = (double[])vectors[rng.Next(vectors.Count)].Clone();
            }
            else
            {
                for (int d = 0; d < dims; d++)
                    centroids[c][d] = sums[c][d] / counts[c];
            }
        }
    }

    // Replace centroids with medoids (nearest real vector)
    Console.WriteLine("  Replacing centroids with medoids...");
    for (int c = 0; c < k; c++)
    {
        var bestDist = double.MaxValue;
        double[]? bestVec = null;
        for (int v = 0; v < vectors.Count; v++)
        {
            if (assignments[v] != c) continue;
            var dist = SquaredDistance(vectors[v], centroids[c]);
            if (dist < bestDist)
            {
                bestDist = dist;
                bestVec = vectors[v];
            }
        }
        if (bestVec is not null)
            centroids[c] = bestVec;
    }

    return centroids;
}

static double SquaredDistance(double[] a, double[] b)
{
    double sum = 0;
    for (int i = 0; i < a.Length; i++)
    {
        var diff = a[i] - b[i];
        sum += diff * diff;
    }
    return sum;
}

record Reference(
    [property: JsonPropertyName("vector")] double[] Vector,
    [property: JsonPropertyName("label")] string Label);

[JsonSerializable(typeof(List<Reference>))]
internal partial class RefJsonContext : JsonSerializerContext { }
