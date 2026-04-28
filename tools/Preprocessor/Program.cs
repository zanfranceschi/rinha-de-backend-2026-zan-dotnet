using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;

const int Scale = 32_767;
const int Dim = 14;
const int Stride = 16;     // 14 dims + 2 zero pad (cacheline-friendly)
const int LeafSize = 32;
const int VpNone = -1;
const int PivotSampleSize = 64;
const int MaxStackCapacity = 256;

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
    Console.WriteLine($"Clustering: {fraudK} fraud + {legitK} legit centroids");

    fraudCentroids = KMeans(fraud, fraudK, maxIterations);
    legitCentroids = KMeans(legit, legitK, maxIterations);
}
else
{
    fraudCentroids = fraud.ToArray();
    legitCentroids = legit.ToArray();
}

var total = fraudCentroids.Length + legitCentroids.Length;
var vectors = new short[total * Stride];
var labels = new byte[total];

short Quantize(double v)
{
    var q = Math.Round(v * Scale);
    if (q > short.MaxValue) q = short.MaxValue;
    if (q < short.MinValue) q = short.MinValue;
    return (short)q;
}

for (int i = 0; i < fraudCentroids.Length; i++)
{
    int b = i * Stride;
    for (int d = 0; d < Dim; d++) vectors[b + d] = Quantize(fraudCentroids[i][d]);
    labels[i] = 1;
}
for (int i = 0; i < legitCentroids.Length; i++)
{
    int b = (fraudCentroids.Length + i) * Stride;
    for (int d = 0; d < Dim; d++) vectors[b + d] = Quantize(legitCentroids[i][d]);
    labels[fraudCentroids.Length + i] = 0;
}

Console.WriteLine($"Building VP-tree (N={total}, leaf_size={LeafSize})...");
var watch = System.Diagnostics.Stopwatch.StartNew();
var nodes = new List<VpNode>();
var leafIndices = new List<int>();
var pointIndices = Enumerable.Range(0, total).ToArray();
BuildVpNode(vectors, pointIndices.AsSpan(), nodes, leafIndices, 1);
watch.Stop();
Console.WriteLine($"VP-tree: {nodes.Count} nodes, {leafIndices.Count} leaf entries in {watch.ElapsedMilliseconds} ms");

// Format v5: [int32 count][N*Stride shorts][N labels][int32 nodeCount][nodes][int32 indCount][indices]
var outputPath = Path.Combine(resourcesPath, "references.bin");
using (var fs = File.Create(outputPath))
using (var bw = new BinaryWriter(fs))
{
    bw.Write(total);
    fs.Write(MemoryMarshal.AsBytes(vectors.AsSpan()));
    fs.Write(labels);
    bw.Write(nodes.Count);
    fs.Write(MemoryMarshal.AsBytes(CollectionsMarshal.AsSpan(nodes)));
    bw.Write(leafIndices.Count);
    fs.Write(MemoryMarshal.AsBytes(CollectionsMarshal.AsSpan(leafIndices)));
}
var fileSize = new FileInfo(outputPath).Length;
Console.WriteLine($"Written {total} references to {outputPath} ({fileSize:N0} bytes, ~{fileSize / 1024.0 / 1024.0:F1} MB)");

int BuildVpNode(short[] vectors, Span<int> indices, List<VpNode> nodes, List<int> leafIndices, int depth)
{
    int nodeIdx = nodes.Count;
    nodes.Add(default);

    if (indices.Length <= LeafSize || depth >= MaxStackCapacity)
    {
        FinalizeLeaf(nodeIdx, indices, nodes, leafIndices);
        return nodeIdx;
    }

    int pivotPos = ChoosePivotPosition(vectors, indices);
    (indices[pivotPos], indices[indices.Length - 1]) = (indices[indices.Length - 1], indices[pivotPos]);
    int pivotIdx = indices[indices.Length - 1];
    var candidates = indices.Slice(0, indices.Length - 1);

    if (candidates.Length == 0)
    {
        FinalizeLeaf(nodeIdx, indices, nodes, leafIndices);
        return nodeIdx;
    }

    var distances = new PointDist[candidates.Length];
    for (int i = 0; i < candidates.Length; i++)
        distances[i] = new PointDist { Idx = candidates[i], Dist = L2Squared(vectors, pivotIdx, candidates[i]) };

    int medianPos = distances.Length / 2;
    NthElement(distances, medianPos);
    long radius = distances[medianPos].Dist;

    int leftCount = 0;
    foreach (var pd in distances)
        if (pd.Dist <= radius) candidates[leftCount++] = pd.Idx;

    if (leftCount == 0 || leftCount == distances.Length)
    {
        FinalizeLeaf(nodeIdx, indices, nodes, leafIndices);
        return nodeIdx;
    }

    int writePos = leftCount;
    foreach (var pd in distances)
        if (pd.Dist > radius) candidates[writePos++] = pd.Idx;

    int left = BuildVpNode(vectors, candidates.Slice(0, leftCount), nodes, leafIndices, depth + 1);
    int right = BuildVpNode(vectors, candidates.Slice(leftCount), nodes, leafIndices, depth + 1);

    nodes[nodeIdx] = new VpNode
    {
        PivotIdx = pivotIdx,
        Radius = radius,
        Left = left,
        Right = right,
        LeafStart = 0,
        LeafLen = 0,
    };
    return nodeIdx;
}

void FinalizeLeaf(int nodeIdx, Span<int> indices, List<VpNode> nodes, List<int> leafIndices)
{
    int start = leafIndices.Count;
    foreach (int idx in indices) leafIndices.Add(idx);
    nodes[nodeIdx] = new VpNode
    {
        PivotIdx = VpNone,
        Radius = 0,
        Left = VpNone,
        Right = VpNone,
        LeafStart = start,
        LeafLen = indices.Length,
    };
}

int ChoosePivotPosition(short[] vectors, Span<int> indices)
{
    int sampleLen = Math.Min(indices.Length, PivotSampleSize);
    if (sampleLen <= 1) return 0;

    int step = (indices.Length + sampleLen - 1) / sampleLen;
    Span<int> sampled = stackalloc int[PivotSampleSize];
    for (int i = 0; i < sampleLen; i++) sampled[i] = Math.Min(i * step, indices.Length - 1);

    int bestPos = sampled[0];
    double bestMean = double.NegativeInfinity;

    for (int i = 0; i < sampleLen; i++)
    {
        int candIdx = indices[sampled[i]];
        long total = 0;
        int compCount = 0;
        for (int j = 0; j < sampleLen; j++)
        {
            if (i == j) continue;
            total += L2Squared(vectors, candIdx, indices[sampled[j]]);
            compCount++;
        }
        if (compCount == 0) continue;
        double mean = (double)total / compCount;
        if (mean > bestMean)
        {
            bestMean = mean;
            bestPos = sampled[i];
        }
    }
    return bestPos;
}

long L2Squared(short[] vectors, int aIdx, int bIdx)
{
    int aBase = aIdx * Stride;
    int bBase = bIdx * Stride;
    long total = 0;
    for (int i = 0; i < Stride; i++)
    {
        long diff = vectors[aBase + i] - vectors[bBase + i];
        total += diff * diff;
    }
    return total;
}

void NthElement(PointDist[] arr, int n)
{
    int lo = 0, hi = arr.Length - 1;
    while (lo < hi)
    {
        long pivot = arr[(lo + hi) >> 1].Dist;
        int i = lo - 1, j = hi + 1;
        while (true)
        {
            do { i++; } while (arr[i].Dist < pivot);
            do { j--; } while (arr[j].Dist > pivot);
            if (i >= j) break;
            (arr[i], arr[j]) = (arr[j], arr[i]);
        }
        if (j < n) lo = j + 1;
        else hi = j;
    }
}

static double[][] KMeans(List<double[]> vectors, int k, int maxIterations)
{
    var dims = vectors[0].Length;
    var rng = new Random(42);
    var centroids = new double[k][];
    var indices = Enumerable.Range(0, vectors.Count).OrderBy(_ => rng.Next()).Take(k).ToList();
    for (int i = 0; i < k; i++) centroids[i] = (double[])vectors[indices[i]].Clone();
    var assignments = new int[vectors.Count];

    for (int iter = 0; iter < maxIterations; iter++)
    {
        var changed = 0;
        for (int v = 0; v < vectors.Count; v++)
        {
            var nearest = 0;
            var nearestDist = SquaredDistance(vectors[v], centroids[0]);
            for (int c = 1; c < k; c++)
            {
                var dist = SquaredDistance(vectors[v], centroids[c]);
                if (dist < nearestDist) { nearestDist = dist; nearest = c; }
            }
            if (assignments[v] != nearest) { assignments[v] = nearest; changed++; }
        }
        if (changed == 0) break;

        var sums = new double[k][];
        var counts = new int[k];
        for (int c = 0; c < k; c++) sums[c] = new double[dims];
        for (int v = 0; v < vectors.Count; v++)
        {
            var c = assignments[v];
            counts[c]++;
            for (int d = 0; d < dims; d++) sums[c][d] += vectors[v][d];
        }
        for (int c = 0; c < k; c++)
        {
            if (counts[c] == 0) centroids[c] = (double[])vectors[rng.Next(vectors.Count)].Clone();
            else for (int d = 0; d < dims; d++) centroids[c][d] = sums[c][d] / counts[c];
        }
    }

    for (int c = 0; c < k; c++)
    {
        var bestDist = double.MaxValue;
        double[]? bestVec = null;
        for (int v = 0; v < vectors.Count; v++)
        {
            if (assignments[v] != c) continue;
            var dist = SquaredDistance(vectors[v], centroids[c]);
            if (dist < bestDist) { bestDist = dist; bestVec = vectors[v]; }
        }
        if (bestVec is not null) centroids[c] = bestVec;
    }
    return centroids;
}

static double SquaredDistance(double[] a, double[] b)
{
    double sum = 0;
    for (int i = 0; i < a.Length; i++) { var diff = a[i] - b[i]; sum += diff * diff; }
    return sum;
}

[StructLayout(LayoutKind.Sequential)]
struct VpNode
{
    public int PivotIdx;
    public long Radius;
    public int Left;
    public int Right;
    public int LeafStart;
    public int LeafLen;
}

struct PointDist
{
    public int Idx;
    public long Dist;
}

record Reference(
    [property: JsonPropertyName("vector")] double[] Vector,
    [property: JsonPropertyName("label")] string Label);

[JsonSerializable(typeof(List<Reference>))]
internal partial class RefJsonContext : JsonSerializerContext { }
