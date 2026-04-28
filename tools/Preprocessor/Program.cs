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

// Write binary format v4 (quantized i8 SoA bucketizado):
// [int32 count]
// [14 × count sbytes — SoA, refs ordenadas por bucket key]
// [count bytes labels — mesma ordem]
// [160 × int32 bucketCount]
const int Scale = 127;
const int Dim = 14;
const int MccBuckets = 10;
const int NumBuckets = 16 * MccBuckets;

sbyte Quantize(double v)
{
    var q = Math.Round(v * Scale);
    if (q > sbyte.MaxValue) q = sbyte.MaxValue;
    if (q < sbyte.MinValue) q = sbyte.MinValue;
    return (sbyte)q;
}

int MakeBucketKey(sbyte d5, sbyte d9, sbyte d10, sbyte d11, sbyte d12)
{
    int hasLast = d5 >= 0 ? 1 : 0;
    int online = d9 > 64 ? 1 : 0;
    int cardPresent = d10 > 64 ? 1 : 0;
    int unknown = d11 > 64 ? 1 : 0;
    int mcc = d12 <= 0 ? 0 : Math.Min(MccBuckets - 1, d12 * MccBuckets / 128);
    return ((((hasLast * 2 + online) * 2 + cardPresent) * 2 + unknown) * MccBuckets) + mcc;
}

var outputPath = Path.Combine(resourcesPath, "references.bin");
var total = fraudCentroids.Length + legitCentroids.Length;

// Materializa quantizado em SoA na ordem original.
var dims = new sbyte[Dim][];
for (int d = 0; d < Dim; d++) dims[d] = new sbyte[total];
var labels = new byte[total];

int idx = 0;
foreach (var v in fraudCentroids)
{
    for (int d = 0; d < Dim; d++) dims[d][idx] = Quantize(v[d]);
    labels[idx] = 1;
    idx++;
}
foreach (var v in legitCentroids)
{
    for (int d = 0; d < Dim; d++) dims[d][idx] = Quantize(v[d]);
    labels[idx] = 0;
    idx++;
}

// Calcula bucket key por ref e conta por bucket.
var keys = new int[total];
var bucketCount = new int[NumBuckets];
for (int i = 0; i < total; i++)
{
    keys[i] = MakeBucketKey(dims[5][i], dims[9][i], dims[10][i], dims[11][i], dims[12][i]);
    bucketCount[keys[i]]++;
}

// Permutação por counting sort.
var bucketStart = new int[NumBuckets];
{
    int acc = 0;
    for (int b = 0; b < NumBuckets; b++) { bucketStart[b] = acc; acc += bucketCount[b]; }
}
var perm = new int[total];
{
    var writePos = (int[])bucketStart.Clone();
    for (int i = 0; i < total; i++)
        perm[writePos[keys[i]]++] = i;
}

// Aplica permutação (uma dim por vez pra reduzir pico de memória).
for (int d = 0; d < Dim; d++)
{
    var src = dims[d];
    var dst = new sbyte[total];
    for (int i = 0; i < total; i++) dst[i] = src[perm[i]];
    dims[d] = dst;
}
var newLabels = new byte[total];
for (int i = 0; i < total; i++) newLabels[i] = labels[perm[i]];
labels = newLabels;

using (var fs = File.Create(outputPath))
using (var bw = new BinaryWriter(fs))
{
    bw.Write(total);
    for (int d = 0; d < Dim; d++)
    {
        var bytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(dims[d].AsSpan());
        fs.Write(bytes);
    }
    fs.Write(labels);
    for (int b = 0; b < NumBuckets; b++) bw.Write(bucketCount[b]);
}

// Diagnóstico: distribuição de buckets.
int nonEmpty = 0, maxB = 0, minB = int.MaxValue;
for (int b = 0; b < NumBuckets; b++)
{
    if (bucketCount[b] > 0) { nonEmpty++; if (bucketCount[b] > maxB) maxB = bucketCount[b]; if (bucketCount[b] < minB) minB = bucketCount[b]; }
}
var fileSize = new FileInfo(outputPath).Length;
Console.WriteLine($"Written {total} references to {outputPath} ({fileSize} bytes, ~{fileSize / 1024.0 / 1024.0:F1} MB)");
Console.WriteLine($"Buckets: {nonEmpty}/{NumBuckets} usados, min={minB} max={maxB} avg={total / nonEmpty}");

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
