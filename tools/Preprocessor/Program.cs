using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text.Json;
using System.Text.Json.Serialization;

const int Scale = 10_000;
const int Dim = 14;
const int Stride = 16;     // 14 dims + 2 zero pad (cacheline-friendly)

var resourcesPath = args.Length > 0
    ? args[0]
    : Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "resources");

var nClusters = args.Length > 1 ? int.Parse(args[1]) : 2048;
var trainSample = args.Length > 2 ? int.Parse(args[2]) : 131072;
var trainIters = args.Length > 3 ? int.Parse(args[3]) : 10;

Console.WriteLine($"Loading references from {resourcesPath}...");
List<Reference> references;
using (var gz = new GZipStream(File.OpenRead(Path.Combine(resourcesPath, "references.json.gz")), CompressionMode.Decompress))
{
    references = JsonSerializer.Deserialize(gz, RefJsonContext.Default.ListReference)!;
}
Console.WriteLine($"Loaded {references.Count} references.");

var total = references.Count;
var vectors = new short[total * Stride];
var labels = new byte[total];

short Quantize(double v)
{
    var q = Math.Round(v * Scale);
    if (q > short.MaxValue) q = short.MaxValue;
    if (q < short.MinValue) q = short.MinValue;
    return (short)q;
}

for (int i = 0; i < total; i++)
{
    int b = i * Stride;
    var vec = references[i].Vector;
    for (int d = 0; d < Dim; d++) vectors[b + d] = Quantize(vec[d]);
    labels[i] = references[i].Label == "fraud" ? (byte)1 : (byte)0;
}

// --- Build IVF index ---
Console.WriteLine($"Building IVF index (N={total}, nClusters={nClusters})...");
var watch = System.Diagnostics.Stopwatch.StartNew();

// K-Means on sample, then assign all vectors
var sampleSize = trainSample == 0 ? total : Math.Min(trainSample, total);
Console.WriteLine($"Training KMeans on {(sampleSize == total ? "all" : $"sample of {sampleSize}")} vectors, {trainIters} iters...");
var clusterCentroids = KMeansShort(vectors, total, nClusters, trainIters, sampleSize);

// Assign each vector to nearest centroid
var assignments = new int[total];
Parallel.For(0, total, i =>
{
    long bestDist = long.MaxValue;
    int bestC = 0;
    var qVec = Vector256.LoadUnsafe(ref vectors[i * Stride]);
    for (int c = 0; c < nClusters; c++)
    {
        long dist = L2SquaredSimd(qVec, clusterCentroids.shortCentroids, c * Stride);
        if (dist < bestDist) { bestDist = dist; bestC = c; }
    }
    assignments[i] = bestC;
});

// Build cluster member lists
var clusterMembers = new List<int>[nClusters];
for (int c = 0; c < nClusters; c++) clusterMembers[c] = new List<int>();
for (int i = 0; i < total; i++) clusterMembers[assignments[i]].Add(i);

var clusterSizes = new int[nClusters];
for (int c = 0; c < nClusters; c++) clusterSizes[c] = clusterMembers[c].Count;

// Build cumulative offsets (nClusters+1, last = total)
var clusterOffsets = new int[nClusters + 1];
for (int c = 0; c < nClusters; c++)
    clusterOffsets[c + 1] = clusterOffsets[c] + clusterSizes[c];

// Build cluster-sorted order and column-major vectors
var sortedOrder = new int[total]; // original index in cluster order
int pos = 0;
for (int c = 0; c < nClusters; c++)
    foreach (var idx in clusterMembers[c])
        sortedOrder[pos++] = idx;

// Column-major: Dim arrays of N shorts
var colMajor = new short[Dim * total];
var sortedLabels = new byte[total];
for (int i = 0; i < total; i++)
{
    int origIdx = sortedOrder[i];
    int srcBase = origIdx * Stride;
    for (int d = 0; d < Dim; d++)
        colMajor[d * total + i] = vectors[srcBase + d];
    sortedLabels[i] = labels[origIdx];
}

// Compute bounding boxes per cluster (min/max per dim)
var bboxMin = new short[nClusters * Dim];
var bboxMax = new short[nClusters * Dim];
for (int c = 0; c < nClusters; c++)
{
    int off = clusterOffsets[c];
    int sz = clusterSizes[c];
    for (int d = 0; d < Dim; d++)
    {
        short mn = short.MaxValue, mx = short.MinValue;
        int dimBase = d * total;
        for (int i = 0; i < sz; i++)
        {
            short v = colMajor[dimBase + off + i];
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        bboxMin[c * Dim + d] = sz > 0 ? mn : (short)0;
        bboxMax[c * Dim + d] = sz > 0 ? mx : (short)0;
    }
}

watch.Stop();
Console.WriteLine($"IVF: {nClusters} clusters in {watch.ElapsedMilliseconds} ms");
for (int c = 0; c < Math.Min(10, nClusters); c++)
    Console.WriteLine($"  cluster[{c}]: {clusterSizes[c]} members");
Console.WriteLine($"  total members: {total}");

// Format IVF v2:
//   [int32 count][int32 nClusters]
//   [nClusters * Stride * float32 (centroids)]
//   [nClusters * Dim * int16 (bbox_min)]
//   [nClusters * Dim * int16 (bbox_max)]
//   [(nClusters+1) * int32 (cumulative offsets)]
//   [Dim * count * int16 (column-major vectors, cluster-sorted)]
//   [count * byte (labels, cluster-sorted)]
var outputPath = Path.Combine(resourcesPath, "references.bin");
using (var fs = File.Create(outputPath))
using (var bw = new BinaryWriter(fs))
{
    bw.Write(total);
    bw.Write(nClusters);

    fs.Write(MemoryMarshal.AsBytes(clusterCentroids.floatCentroids.AsSpan()));
    fs.Write(MemoryMarshal.AsBytes(bboxMin.AsSpan()));
    fs.Write(MemoryMarshal.AsBytes(bboxMax.AsSpan()));
    fs.Write(MemoryMarshal.AsBytes(clusterOffsets.AsSpan()));
    fs.Write(MemoryMarshal.AsBytes(colMajor.AsSpan()));
    fs.Write(sortedLabels);
}
var fileSize = new FileInfo(outputPath).Length;
Console.WriteLine($"Written {total} references to {outputPath} ({fileSize:N0} bytes, ~{fileSize / 1024.0 / 1024.0:F1} MB)");

// --- K-Means on sample with deterministic init ---
// Returns both float (for serialization) and short (for final assignment) centroids
(float[] floatCentroids, short[] shortCentroids) KMeansShort(short[] vecs, int n, int numClusters, int iters, int sampleSize)
{
    numClusters = Math.Min(numClusters, n);
    sampleSize = Math.Clamp(sampleSize, numClusters, n);
    var centroids = new double[numClusters * Stride];

    // Build sample indices (evenly spaced)
    var sampleIdx = new int[sampleSize];
    for (int i = 0; i < sampleSize; i++)
        sampleIdx[i] = (int)((long)i * n / sampleSize);

    // Deterministic init: pick evenly spaced samples as initial centroids
    Console.WriteLine($"  Deterministic init ({numClusters} centroids from {sampleSize} samples)...");
    var initSw = System.Diagnostics.Stopwatch.StartNew();
    for (int c = 0; c < numClusters; c++)
    {
        int si = (int)((long)c * sampleSize / numClusters);
        int srcBase = sampleIdx[si] * Stride;
        int dstBase = c * Stride;
        for (int d = 0; d < Stride; d++) centroids[dstBase + d] = vecs[srcBase + d];
    }
    Console.WriteLine($"  Init done in {initSw.ElapsedMilliseconds}ms");

    var assign = new int[sampleSize];
    var counts = new int[numClusters];
    var sums = new double[numClusters * Stride];
    var qCentroids = new short[numClusters * Stride];
    var prevAssign = new int[sampleSize];

    for (int iter = 0; iter < iters; iter++)
    {
        // Quantize centroids for SIMD assign
        for (int i = 0; i < centroids.Length; i++)
        {
            var q = Math.Round(centroids[i]);
            if (q > short.MaxValue) q = short.MaxValue;
            if (q < short.MinValue) q = short.MinValue;
            qCentroids[i] = (short)q;
        }

        // Assign sample vectors to nearest centroid
        Parallel.For(0, sampleSize, i =>
        {
            long bestDist = long.MaxValue;
            int bestC = 0;
            var qVec = Vector256.LoadUnsafe(ref vecs[sampleIdx[i] * Stride]);
            for (int c = 0; c < numClusters; c++)
            {
                long dist = L2SquaredSimd(qVec, qCentroids, c * Stride);
                if (dist < bestDist) { bestDist = dist; bestC = c; }
            }
            assign[i] = bestC;
        });

        // Early stopping
        int changed = 0;
        for (int i = 0; i < sampleSize; i++)
            if (assign[i] != prevAssign[i]) changed++;
        Array.Copy(assign, prevAssign, sampleSize);

        // Recompute centroids from sample
        Array.Clear(counts);
        Array.Clear(sums);
        for (int i = 0; i < sampleSize; i++)
        {
            int c = assign[i];
            counts[c]++;
            int iBase = sampleIdx[i] * Stride;
            int cb = c * Stride;
            for (int d = 0; d < Stride; d++) sums[cb + d] += vecs[iBase + d];
        }
        for (int c = 0; c < numClusters; c++)
        {
            if (counts[c] == 0) continue;
            int cb = c * Stride;
            for (int d = 0; d < Stride; d++) centroids[cb + d] = sums[cb + d] / counts[c];
        }
        Console.WriteLine($"  IVF KMeans iter {iter}: {changed} changed ({100.0*changed/sampleSize:F2}%)");

        if (changed == 0)
        {
            Console.WriteLine($"  Converged at iter {iter}");
            break;
        }
    }

    // Quantize final centroids for full-dataset assignment
    var shortResult = new short[numClusters * Stride];
    for (int i = 0; i < centroids.Length; i++)
    {
        var q = Math.Round(centroids[i]);
        if (q > short.MaxValue) q = short.MaxValue;
        if (q < short.MinValue) q = short.MinValue;
        shortResult[i] = (short)q;
    }

    // Float centroids for serialization
    var floatResult = new float[numClusters * Stride];
    for (int i = 0; i < centroids.Length; i++)
        floatResult[i] = (float)centroids[i];

    return (floatResult, shortResult);
}

static long L2SquaredSimd(Vector256<short> qVec, short[] targets, int offset)
{
    var v = Vector256.LoadUnsafe(ref targets[offset]);
    var (aLo, aHi) = Vector256.Widen(qVec);
    var (bLo, bHi) = Vector256.Widen(v);
    var dLo = aLo - bLo;
    var dHi = aHi - bHi;

    var sqLoEven = Avx2.Multiply(dLo, dLo);
    var sqLoOdd  = Avx2.Multiply(
        Avx2.ShiftRightLogical(dLo.AsInt64(), 32).AsInt32(),
        Avx2.ShiftRightLogical(dLo.AsInt64(), 32).AsInt32());
    var sqHiEven = Avx2.Multiply(dHi, dHi);
    var sqHiOdd  = Avx2.Multiply(
        Avx2.ShiftRightLogical(dHi.AsInt64(), 32).AsInt32(),
        Avx2.ShiftRightLogical(dHi.AsInt64(), 32).AsInt32());

    var sum = (sqLoEven + sqLoOdd) + (sqHiEven + sqHiOdd);
    return Vector256.Sum(sum);
}

record Reference(
    [property: JsonPropertyName("vector")] double[] Vector,
    [property: JsonPropertyName("label")] string Label);

[JsonSerializable(typeof(List<Reference>))]
internal partial class RefJsonContext : JsonSerializerContext { }
