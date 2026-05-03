using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

public unsafe class DataLoader : IDisposable
{
    public const int Scale = 10_000;
    public const int Dim = 14;
    public const int Stride = 16;       // 14 dims + 2 zero pad (centroids only)
    public const int K = 5;

    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public byte[] Labels { get; }
    public int Count { get; }
    public int NClusters { get; }

    public float* Centroids { get; }         // NClusters * Stride
    public short* BboxMin { get; }           // NClusters * Dim
    public short* BboxMax { get; }           // NClusters * Dim
    public int* Offsets { get; }             // NClusters + 1 (cumulative)
    public short*[] Dims { get; }            // Dim pointers, each to Count shorts (column-major)

    private short* _dimData;                 // single allocation for all column-major data

    public DataLoader(string resourcesPath)
    {
        MccRisk = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "mcc_risk.json")),
            AppJsonContext.Default.DictionaryStringDouble)!;

        Normalization = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "normalization.json")),
            AppJsonContext.Default.NormalizationConfig)!;

        // Format IVF v2:
        //   [int32 count][int32 nClusters]
        //   [nClusters * Stride * float32 (centroids)]
        //   [nClusters * Dim * int16 (bbox_min)]
        //   [nClusters * Dim * int16 (bbox_max)]
        //   [(nClusters+1) * int32 (cumulative offsets)]
        //   [Dim * count * int16 (column-major vectors, cluster-sorted)]
        //   [count * byte (labels, cluster-sorted)]
        using var fs = File.OpenRead(Path.Combine(resourcesPath, "references.bin"));

        Span<byte> intBuf = stackalloc byte[4];

        fs.ReadExactly(intBuf);
        Count = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

        fs.ReadExactly(intBuf);
        NClusters = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

        long centroidBytes = (long)NClusters * Stride * sizeof(float);
        Centroids = (float*)NativeMemory.AllocZeroed((nuint)centroidBytes);
        ReadExact(fs, (byte*)Centroids, centroidBytes);

        long bboxBytes = (long)NClusters * Dim * sizeof(short);
        BboxMin = (short*)NativeMemory.AllocZeroed((nuint)bboxBytes);
        ReadExact(fs, (byte*)BboxMin, bboxBytes);
        BboxMax = (short*)NativeMemory.AllocZeroed((nuint)bboxBytes);
        ReadExact(fs, (byte*)BboxMax, bboxBytes);

        long offsetBytes = (long)(NClusters + 1) * sizeof(int);
        Offsets = (int*)NativeMemory.AllocZeroed((nuint)offsetBytes);
        ReadExact(fs, (byte*)Offsets, offsetBytes);

        long dimDataBytes = (long)Dim * Count * sizeof(short);
        _dimData = (short*)NativeMemory.AllocZeroed((nuint)dimDataBytes);
        ReadExact(fs, (byte*)_dimData, dimDataBytes);

        Dims = new short*[Dim];
        for (int d = 0; d < Dim; d++)
            Dims[d] = _dimData + (long)d * Count;

        Labels = new byte[Count];
        fs.ReadExactly(Labels);
    }

    private static void ReadExact(Stream s, byte* dst, long total)
    {
        long read = 0;
        while (read < total)
        {
            int chunk = (int)Math.Min(total - read, int.MaxValue / 2);
            int got = s.Read(new Span<byte>(dst + read, chunk));
            if (got == 0) throw new InvalidDataException("references.bin truncado");
            read += got;
        }
    }

    public void Dispose()
    {
        if (Centroids != null) NativeMemory.Free(Centroids);
        if (BboxMin != null) NativeMemory.Free(BboxMin);
        if (BboxMax != null) NativeMemory.Free(BboxMax);
        if (Offsets != null) NativeMemory.Free(Offsets);
        if (_dimData != null) NativeMemory.Free(_dimData);
    }
}

public static class Vectorizer
{
    private static readonly short[] HourLut = BuildLut(24, 23.0);
    private static readonly short[] DowLut = BuildLut(7, 6.0);

    private static short[] BuildLut(int n, double divisor)
    {
        var arr = new short[n];
        for (int i = 0; i < n; i++)
            arr[i] = (short)Math.Round(i / divisor * DataLoader.Scale);
        return arr;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTime ParseIsoUtc(string s)
    {
        int y = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        int M = (s[5] - '0') * 10 + (s[6] - '0');
        int d = (s[8] - '0') * 10 + (s[9] - '0');
        int h = (s[11] - '0') * 10 + (s[12] - '0');
        int m = (s[14] - '0') * 10 + (s[15] - '0');
        int sec = (s[17] - '0') * 10 + (s[18] - '0');
        return new DateTime(y, M, d, h, m, sec, DateTimeKind.Utc);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Vectorize(FraudRequest req, NormalizationConfig norm,
        Dictionary<string, double> mccRisk, Span<short> output)
    {
        var requestedAt = ParseIsoUtc(req.Transaction.RequestedAt);
        var dow = ((int)requestedAt.DayOfWeek + 6) % 7;

        output[0] = Q(Clamp(req.Transaction.Amount / norm.MaxAmount));
        output[1] = Q(Clamp(req.Transaction.Installments / norm.MaxInstallments));
        output[2] = Q(Clamp((req.Transaction.Amount / req.Customer.AvgAmount) / norm.AmountVsAvgRatio));
        output[3] = HourLut[requestedAt.Hour];
        output[4] = DowLut[dow];

        if (req.LastTransaction is not null)
        {
            var lastTs = ParseIsoUtc(req.LastTransaction.Timestamp);
            var minutes = (requestedAt - lastTs).TotalMinutes;
            output[5] = Q(Clamp(minutes / norm.MaxMinutes));
            output[6] = Q(Clamp(req.LastTransaction.KmFromCurrent / norm.MaxKm));
        }
        else
        {
            output[5] = (short)-DataLoader.Scale;
            output[6] = (short)-DataLoader.Scale;
        }

        output[7] = Q(Clamp(req.Terminal.KmFromHome / norm.MaxKm));
        output[8] = Q(Clamp(req.Customer.TxCount24h / norm.MaxTxCount24h));
        output[9]  = req.Terminal.IsOnline ? (short)DataLoader.Scale : (short)0;
        output[10] = req.Terminal.CardPresent ? (short)DataLoader.Scale : (short)0;
        output[11] = req.Customer.KnownMerchants.Contains(req.Merchant.Id) ? (short)0 : (short)DataLoader.Scale;
        output[12] = Q(mccRisk.GetValueOrDefault(req.Merchant.Mcc, 0.5));
        output[13] = Q(Clamp(req.Merchant.AvgAmount / norm.MaxMerchantAvgAmount));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double Clamp(double x) => Math.Clamp(x, 0.0, 1.0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static short Q(double v)
    {
        var q = Math.Round(v * DataLoader.Scale);
        if (q > short.MaxValue) q = short.MaxValue;
        if (q < short.MinValue) q = short.MinValue;
        return (short)q;
    }
}

public unsafe class FraudDetector
{
    public readonly DataLoader Data;
    private readonly float* _centroids;
    private readonly short* _bboxMin;
    private readonly short* _bboxMax;
    private readonly int* _offsets;
    private readonly short*[] _dims;
    private readonly byte[] _labels;
    private readonly int _nClusters;

    // Dimension order for early exit: high-variance continuous dims first, binary/cyclic last
    // (same order as the reference C implementation)
    private static readonly int[] DimOrder = { 5, 6, 2, 0, 7, 8, 11, 12, 9, 10, 1, 13, 3, 4 };

    public int NProbe { get; }
    public int NProbeRetryExtra { get; }
    public int K { get; }
    public bool Instrumented { get; }

    public static readonly byte[][] PrecomputedResponses = BuildResponses();

    public FraudDetector(DataLoader data)
    {
        Data = data;
        _centroids = data.Centroids;
        _bboxMin = data.BboxMin;
        _bboxMax = data.BboxMax;
        _offsets = data.Offsets;
        _dims = data.Dims;
        _labels = data.Labels;
        _nClusters = data.NClusters;

        var nprobeEnv = Environment.GetEnvironmentVariable("NPROBE");
        NProbe = nprobeEnv != null ? int.Parse(nprobeEnv) : 8;

        var retryEnv = Environment.GetEnvironmentVariable("NPROBE_RETRY_EXTRA");
        NProbeRetryExtra = retryEnv != null ? int.Parse(retryEnv) : 100;

        var kEnv = Environment.GetEnvironmentVariable("KNN_K");
        K = kEnv != null ? int.Parse(kEnv) : DataLoader.K;

        Instrumented = string.Equals(Environment.GetEnvironmentVariable("INSTRUMENTED"), "true", StringComparison.OrdinalIgnoreCase);

        Console.WriteLine($"IVF: {_nClusters} clusters, nprobe={NProbe}, instrumented={Instrumented}");
    }

    private static byte[][] BuildResponses()
    {
        var arr = new byte[6][];
        for (int n = 0; n <= 5; n++)
        {
            var score = n / 5.0;
            var resp = new FraudResponse(score < 0.6, score);
            arr[n] = JsonSerializer.SerializeToUtf8Bytes(resp, AppJsonContext.Default.FraudResponse);
        }
        return arr;
    }

    public int FraudCount(FraudRequest req)
    {
        long tv0 = Stopwatch.GetTimestamp();
        Span<short> query = stackalloc short[DataLoader.Stride];
        Vectorizer.Vectorize(req, Data.Normalization, Data.MccRisk, query);
        long tv1 = Stopwatch.GetTimestamp();
        if (Instrumented)
            Interlocked.Add(ref _sVectorizeTicks, tv1 - tv0);
        return SearchKnn(query);
    }

    private static long _sDistComps;
    private static long _sSearchCalls;
    private static long _sCentroidTicks;
    private static long _sClusterScanTicks;
    private static long _sRetryTicks;
    private static long _sRetryCount;
    private static long _sVectorizeTicks;

    public int SearchKnn(ReadOnlySpan<short> query)
    {
        int nClusters = _nClusters;
        int nprobe = Math.Min(NProbe, nClusters);
        int k = K;
        var labels = _labels;
        float* centroids = _centroids;
        short* bboxMin = _bboxMin;
        short* bboxMax = _bboxMax;
        int* offsets = _offsets;
        var dims = _dims;

        long t0 = Stopwatch.GetTimestamp();

        // Find nprobe closest centroids
        Span<float> probeDist = stackalloc float[nprobe];
        Span<int> probeIdx = stackalloc int[nprobe];
        probeDist.Fill(float.MaxValue);

        for (int c = 0; c < nClusters; c++)
        {
            float dist = DistByCentroidFloat(query, centroids, c);
            if (dist < probeDist[nprobe - 1])
            {
                int pos = nprobe - 1;
                while (pos > 0 && probeDist[pos - 1] > dist) { probeDist[pos] = probeDist[pos - 1]; probeIdx[pos] = probeIdx[pos - 1]; pos--; }
                probeDist[pos] = dist;
                probeIdx[pos] = c;
            }
        }

        long t1 = Stopwatch.GetTimestamp();

        // Search vectors in the nprobe closest clusters
        Span<long> bestD = stackalloc long[k];
        bestD.Fill(long.MaxValue);
        Span<int> bestId = stackalloc int[k];
        int found = 0;
        int distComps = 0;

        for (int p = 0; p < nprobe; p++)
        {
            int clusterId = probeIdx[p];

            if (found >= k)
            {
                long lb = BboxLowerBound(query, bboxMin, bboxMax, clusterId);
                if (lb > bestD[k - 1]) continue;
            }

            int clusterStart = offsets[clusterId];
            int clusterEnd = offsets[clusterId + 1];
            distComps += clusterEnd - clusterStart;

            ScanClusterSimd(query, dims, clusterStart, clusterEnd, bestD, bestId, ref found, k);
        }

        long t2 = Stopwatch.GetTimestamp();
        long retryTicks = 0;

        // If borderline, find extra centroids and continue search
        int maxProbe = Math.Min(NProbeRetryExtra, nClusters);
        if (maxProbe > nprobe)
        {
            int fc = 0;
            for (int i = 0; i < found; i++)
                fc += labels[bestId[i]];

            if (fc is 2 or 3)
            {
                // Find maxProbe closest centroids, excluding already-visited ones
                Span<float> extraDist = stackalloc float[maxProbe];
                Span<int> extraIdx = stackalloc int[maxProbe];
                extraDist.Fill(float.MaxValue);

                for (int c = 0; c < nClusters; c++)
                {
                    // Skip already probed clusters
                    bool already = false;
                    for (int p = 0; p < nprobe; p++) { if (probeIdx[p] == c) { already = true; break; } }
                    if (already) continue;

                    float dist = DistByCentroidFloat(query, centroids, c);
                    if (dist < extraDist[maxProbe - 1])
                    {
                        int pos = maxProbe - 1;
                        while (pos > 0 && extraDist[pos - 1] > dist) { extraDist[pos] = extraDist[pos - 1]; extraIdx[pos] = extraIdx[pos - 1]; pos--; }
                        extraDist[pos] = dist;
                        extraIdx[pos] = c;
                    }
                }

                int extraCount = Math.Min(maxProbe - nprobe, maxProbe);
                for (int p = 0; p < extraCount; p++)
                {
                    if (extraDist[p] == float.MaxValue) break;
                    int clusterId = extraIdx[p];

                    if (found >= k)
                    {
                        long lb = BboxLowerBound(query, bboxMin, bboxMax, clusterId);
                        if (lb > bestD[k - 1]) continue;
                    }

                    int clusterStart = offsets[clusterId];
                    int clusterEnd = offsets[clusterId + 1];
                    distComps += clusterEnd - clusterStart;

                    ScanClusterSimd(query, dims, clusterStart, clusterEnd, bestD, bestId, ref found, k);
                }

                retryTicks = Stopwatch.GetTimestamp() - t2;
                Interlocked.Increment(ref _sRetryCount);
            }
        }

        if (Instrumented)
        {
            Interlocked.Add(ref _sCentroidTicks, t1 - t0);
            Interlocked.Add(ref _sClusterScanTicks, t2 - t1);
            Interlocked.Add(ref _sRetryTicks, retryTicks);
            Interlocked.Add(ref _sDistComps, distComps);
            var n = Interlocked.Increment(ref _sSearchCalls);
            if (n % 100 == 0)
            {
                double freq = Stopwatch.Frequency;
                double avgVec = Interlocked.Read(ref _sVectorizeTicks) / n / freq * 1_000_000;
                double avgCentroid = Interlocked.Read(ref _sCentroidTicks) / n / freq * 1_000_000;
                double avgScan = Interlocked.Read(ref _sClusterScanTicks) / n / freq * 1_000_000;
                long retries = Interlocked.Read(ref _sRetryCount);
                double avgRetry = retries > 0 ? Interlocked.Read(ref _sRetryTicks) / (double)retries / freq * 1_000_000 : 0;
                Console.WriteLine($"[IVF n={n}] vec={avgVec:F0}us centroid={avgCentroid:F0}us scan={avgScan:F0}us retry={avgRetry:F0}us(x{retries}) distComps={Interlocked.Read(ref _sDistComps)/n}");
            }
        }

        int fraudCount = 0;
        for (int i = 0; i < found; i++)
            fraudCount += labels[bestId[i]];
        return fraudCount;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ScanClusterSimd(
        ReadOnlySpan<short> query, short*[] dims, int clusterStart, int clusterEnd,
        Span<long> bestD, Span<int> bestId, ref int found, int k)
    {
        int i = clusterStart;
        int simdEnd = clusterStart + ((clusterEnd - clusterStart) & ~15); // round down to 16

        // Process 16 vectors at a time
        for (; i < simdEnd; i += 16)
        {
            // Accumulators for 16 vectors (two groups of 8 as int32)
            Vector256<int> accLo = Vector256<int>.Zero;
            Vector256<int> accHi = Vector256<int>.Zero;

            for (int d = 0; d < DataLoader.Dim; d++)
            {
                // Broadcast query[d] to all 16 lanes
                Vector256<short> qv = Vector256.Create(query[d]);
                // Load 16 consecutive values for dimension d
                Vector256<short> dv = Avx2.LoadVector256(dims[d] + i);
                // diff = query - data
                Vector256<short> diff = Avx2.Subtract(qv, dv);

                // Widen to int32 and square-accumulate
                // Lower 8 shorts → 8 int32
                Vector256<int> diffLo = Avx2.ConvertToVector256Int32(diff.GetLower());
                accLo = Avx2.Add(accLo, Avx2.MultiplyLow(diffLo, diffLo));

                // Upper 8 shorts → 8 int32
                Vector256<int> diffHi = Avx2.ConvertToVector256Int32(diff.GetUpper());
                accHi = Avx2.Add(accHi, Avx2.MultiplyLow(diffHi, diffHi));
            }

            // Extract and check each of the 16 distances
            long worstD = found >= k ? bestD[k - 1] : long.MaxValue;

            for (int j = 0; j < 8; j++)
            {
                long dist = (uint)accLo.GetElement(j);
                if (dist < worstD)
                {
                    InsertTopK(bestD, bestId, ref found, i + j, dist, k);
                    worstD = found >= k ? bestD[k - 1] : long.MaxValue;
                }
            }
            for (int j = 0; j < 8; j++)
            {
                long dist = (uint)accHi.GetElement(j);
                if (dist < worstD)
                {
                    InsertTopK(bestD, bestId, ref found, i + 8 + j, dist, k);
                    worstD = found >= k ? bestD[k - 1] : long.MaxValue;
                }
            }
        }

        // Scalar tail for remaining vectors
        for (; i < clusterEnd; i++)
        {
            long worstD = found >= k ? bestD[k - 1] : long.MaxValue;
            long dist = 0;

            for (int d = 0; d < DataLoader.Dim; d++)
            {
                long diff = query[d] - dims[d][i];
                dist += diff * diff;
                if (dist > worstD) goto nextTail;
            }

            InsertTopK(bestD, bestId, ref found, i, dist, k);
            nextTail:;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float DistByCentroidFloat(ReadOnlySpan<short> query, float* centroids, int centroidIdx)
    {
        float* c = centroids + centroidIdx * DataLoader.Stride;
        float sum = 0;
        for (int d = 0; d < DataLoader.Dim; d++)
        {
            float diff = query[d] - c[d];
            sum += diff * diff;
        }
        return sum;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long BboxLowerBound(ReadOnlySpan<short> query, short* bboxMin, short* bboxMax, int clusterId)
    {
        long lb = 0;
        int bboxBase = clusterId * DataLoader.Dim;
        for (int d = 0; d < DataLoader.Dim; d++)
        {
            int q = query[d];
            int lo = bboxMin[bboxBase + d];
            int hi = bboxMax[bboxBase + d];
            int diff = 0;
            if (q < lo) diff = lo - q;
            else if (q > hi) diff = q - hi;
            lb += (long)diff * diff;
        }
        return lb;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void InsertTopK(Span<long> bestD, Span<int> bestId, ref int found, int idx, long dist, int k)
    {
        int insertAt = 0;
        while (insertAt < found && bestD[insertAt] < dist) insertAt++;
        if (insertAt >= k) return;

        int upperBound = Math.Min(found, k - 1);
        for (int i = upperBound - 1; i >= insertAt; i--)
        {
            bestD[i + 1] = bestD[i];
            bestId[i + 1] = bestId[i];
        }
        bestD[insertAt] = dist;
        bestId[insertAt] = idx;
        if (found < k) found++;
    }
}
