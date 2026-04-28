using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

public class DataLoader
{
    public const int Scale = 127;
    public const int Dim = 14;
    public const int MccBuckets = 10;
    public const int NumBuckets = 16 * MccBuckets;
    public const int DefaultScanBudget = 300_000;

    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public sbyte[][] Dims { get; }
    public byte[] Labels { get; }
    public int Count { get; }

    // Bucketing: refs já vêm ordenadas por bucket no arquivo.
    public int[] BucketStart { get; }   // [NumBuckets]
    public int[] BucketEnd { get; }     // [NumBuckets]
    public int[][] BucketOrder { get; } // [NumBuckets][NumBuckets] — ordem de proximidade
    public int ScanBudget { get; }

    public DataLoader(string resourcesPath)
    {
        MccRisk = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "mcc_risk.json")),
            AppJsonContext.Default.DictionaryStringDouble)!;

        Normalization = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "normalization.json")),
            AppJsonContext.Default.NormalizationConfig)!;

        // Formato v4: [int32 count][14 × count sbyte SoA bucketizado][count labels][NumBuckets × int32 bucketCount]
        using var fs = File.OpenRead(Path.Combine(resourcesPath, "references.bin"));
        using var br = new BinaryReader(fs);
        Count = br.ReadInt32();

        Dims = new sbyte[Dim][];
        for (int j = 0; j < Dim; j++)
        {
            Dims[j] = GC.AllocateUninitializedArray<sbyte>(Count, pinned: true);
            var bytes = MemoryMarshal.AsBytes(Dims[j].AsSpan());
            int got = fs.Read(bytes);
            if (got != Count) throw new InvalidDataException($"references.bin truncado na coluna {j}");
        }
        Labels = br.ReadBytes(Count);

        var bucketCount = new int[NumBuckets];
        for (int b = 0; b < NumBuckets; b++) bucketCount[b] = br.ReadInt32();

        BucketStart = new int[NumBuckets];
        BucketEnd = new int[NumBuckets];
        int acc = 0;
        for (int b = 0; b < NumBuckets; b++)
        {
            BucketStart[b] = acc;
            acc += bucketCount[b];
            BucketEnd[b] = acc;
        }
        if (acc != Count) throw new InvalidDataException($"bucketCount soma {acc} != Count {Count}");

        BucketOrder = BuildBucketOrder();

        var budgetStr = Environment.GetEnvironmentVariable("SCAN_BUDGET");
        ScanBudget = (budgetStr is not null && int.TryParse(budgetStr, out var cv) && cv > 0)
            ? cv : DefaultScanBudget;
    }

    // Pra cada bucket de query, ordena os outros buckets por penalty crescente.
    private static int[][] BuildBucketOrder()
    {
        var order = new int[NumBuckets][];
        var pairs = new (int b, int p)[NumBuckets];
        for (int q = 0; q < NumBuckets; q++)
        {
            for (int b = 0; b < NumBuckets; b++) pairs[b] = (b, Penalty(q, b));
            Array.Sort(pairs, (x, y) =>
            {
                int c = x.p.CompareTo(y.p);
                return c != 0 ? c : x.b.CompareTo(y.b);
            });
            var arr = new int[NumBuckets];
            for (int i = 0; i < NumBuckets; i++) arr[i] = pairs[i].b;
            order[q] = arr;
        }
        return order;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Penalty(int qkey, int bkey)
    {
        int qmcc = qkey % MccBuckets;
        int bmcc = bkey % MccBuckets;
        int q4 = qkey / MccBuckets;
        int b4 = bkey / MccBuckets;
        int hamming = BitOperations.PopCount((uint)(q4 ^ b4));
        int mccDist = Math.Abs(qmcc - bmcc);
        return hamming * MccBuckets + mccDist;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int MakeBucketKey(sbyte d5, sbyte d9, sbyte d10, sbyte d11, sbyte d12)
    {
        int hasLast = d5 >= 0 ? 1 : 0;
        int online = d9 > 64 ? 1 : 0;
        int cardPresent = d10 > 64 ? 1 : 0;
        int unknown = d11 > 64 ? 1 : 0;
        int mcc = d12 <= 0 ? 0 : Math.Min(MccBuckets - 1, d12 * MccBuckets / 128);
        return ((((hasLast * 2 + online) * 2 + cardPresent) * 2 + unknown) * MccBuckets) + mcc;
    }
}

public static class Vectorizer
{
    private static readonly sbyte[] HourLut = BuildLut(24, 23.0);
    private static readonly sbyte[] DowLut = BuildLut(7, 6.0);

    private static sbyte[] BuildLut(int n, double divisor)
    {
        var arr = new sbyte[n];
        for (int i = 0; i < n; i++)
            arr[i] = (sbyte)Math.Round(i / divisor * DataLoader.Scale);
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
        Dictionary<string, double> mccRisk, Span<sbyte> output)
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
            output[5] = (sbyte)-DataLoader.Scale;
            output[6] = (sbyte)-DataLoader.Scale;
        }

        output[7] = Q(Clamp(req.Terminal.KmFromHome / norm.MaxKm));
        output[8] = Q(Clamp(req.Customer.TxCount24h / norm.MaxTxCount24h));
        output[9]  = req.Terminal.IsOnline ? (sbyte)DataLoader.Scale : (sbyte)0;
        output[10] = req.Terminal.CardPresent ? (sbyte)DataLoader.Scale : (sbyte)0;
        output[11] = req.Customer.KnownMerchants.Contains(req.Merchant.Id) ? (sbyte)0 : (sbyte)DataLoader.Scale;
        output[12] = Q(mccRisk.GetValueOrDefault(req.Merchant.Mcc, 0.5));
        output[13] = Q(Clamp(req.Merchant.AvgAmount / norm.MaxMerchantAvgAmount));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double Clamp(double x) => Math.Clamp(x, 0.0, 1.0);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static sbyte Q(double v)
    {
        var q = Math.Round(v * DataLoader.Scale);
        if (q > sbyte.MaxValue) q = sbyte.MaxValue;
        if (q < sbyte.MinValue) q = sbyte.MinValue;
        return (sbyte)q;
    }
}

public class FraudDetector
{
    private readonly DataLoader _data;
    public static readonly byte[][] PrecomputedResponses = BuildResponses();

    public FraudDetector(DataLoader data)
    {
        _data = data;
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
        Span<sbyte> query = stackalloc sbyte[DataLoader.Dim];
        Vectorizer.Vectorize(req, _data.Normalization, _data.MccRisk, query);
        int qkey = DataLoader.MakeBucketKey(query[5], query[9], query[10], query[11], query[12]);
        return CountFraudNeighborsBucketed(query, qkey);
    }

    private int CountFraudNeighborsBucketed(ReadOnlySpan<sbyte> query, int qkey)
    {
        var dims = _data.Dims;
        var labels = _data.Labels;
        var order = _data.BucketOrder[qkey];
        var bStart = _data.BucketStart;
        var bEnd = _data.BucketEnd;
        int budget = _data.ScanBudget;

        // Broadcasts da query.
        var q0  = Vector256.Create((short)query[0]);
        var q1  = Vector256.Create((short)query[1]);
        var q2  = Vector256.Create((short)query[2]);
        var q3  = Vector256.Create((short)query[3]);
        var q4  = Vector256.Create((short)query[4]);
        var q5  = Vector256.Create((short)query[5]);
        var q6  = Vector256.Create((short)query[6]);
        var q7  = Vector256.Create((short)query[7]);
        var q8  = Vector256.Create((short)query[8]);
        var q9  = Vector256.Create((short)query[9]);
        var q10 = Vector256.Create((short)query[10]);
        var q11 = Vector256.Create((short)query[11]);
        var q12 = Vector256.Create((short)query[12]);
        var q13 = Vector256.Create((short)query[13]);

        ref var d0  = ref MemoryMarshal.GetArrayDataReference(dims[0]);
        ref var d1  = ref MemoryMarshal.GetArrayDataReference(dims[1]);
        ref var d2  = ref MemoryMarshal.GetArrayDataReference(dims[2]);
        ref var d3  = ref MemoryMarshal.GetArrayDataReference(dims[3]);
        ref var d4  = ref MemoryMarshal.GetArrayDataReference(dims[4]);
        ref var d5  = ref MemoryMarshal.GetArrayDataReference(dims[5]);
        ref var d6  = ref MemoryMarshal.GetArrayDataReference(dims[6]);
        ref var d7  = ref MemoryMarshal.GetArrayDataReference(dims[7]);
        ref var d8  = ref MemoryMarshal.GetArrayDataReference(dims[8]);
        ref var d9  = ref MemoryMarshal.GetArrayDataReference(dims[9]);
        ref var d10 = ref MemoryMarshal.GetArrayDataReference(dims[10]);
        ref var d11 = ref MemoryMarshal.GetArrayDataReference(dims[11]);
        ref var d12 = ref MemoryMarshal.GetArrayDataReference(dims[12]);
        ref var d13 = ref MemoryMarshal.GetArrayDataReference(dims[13]);

        Span<int> bestD = stackalloc int[5] { int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue };
        Span<int> bestId = stackalloc int[5];
        int worst = 0;
        int worstD = int.MaxValue;

        Span<int> dists = stackalloc int[32];
        ref var distsRef = ref MemoryMarshal.GetReference(dists);

        int scanned = 0;
        for (int oi = 0; oi < DataLoader.NumBuckets; oi++)
        {
            int b = order[oi];
            int start = bStart[b];
            int end = bEnd[b];
            int sz = end - start;
            if (sz <= 0) continue;

            int simdEnd = start + (sz & ~31); // múltiplo de 32 dentro do bucket
            int i = start;
            for (; i < simdEnd; i += 32)
            {
                nuint off = (nuint)i;
                var aA = Vector256<int>.Zero;
                var aB = Vector256<int>.Zero;
                var aC = Vector256<int>.Zero;
                var aD = Vector256<int>.Zero;
                var wVec = Vector256.Create(worstD);

                Acc(q5,  ref d5,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q6,  ref d6,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q2,  ref d2,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q0,  ref d0,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q7,  ref d7,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q8,  ref d8,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q11, ref d11, off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q12, ref d12, off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q9,  ref d9,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q10, ref d10, off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q1,  ref d1,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q13, ref d13, off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q3,  ref d3,  off, ref aA, ref aB, ref aC, ref aD);
                if (AllExceed(aA, aB, aC, aD, wVec)) continue;
                Acc(q4,  ref d4,  off, ref aA, ref aB, ref aC, ref aD);

                aA.StoreUnsafe(ref distsRef);
                aB.StoreUnsafe(ref distsRef, 8);
                aC.StoreUnsafe(ref distsRef, 16);
                aD.StoreUnsafe(ref distsRef, 24);

                for (int k = 0; k < 32; k++)
                {
                    int d = dists[k];
                    if (d < worstD)
                    {
                        bestD[worst] = d;
                        bestId[worst] = i + k;
                        worst = 0; worstD = bestD[0];
                        for (int j = 1; j < 5; j++)
                            if (bestD[j] > worstD) { worst = j; worstD = bestD[j]; }
                    }
                }
            }

            // Cauda escalar do bucket.
            for (; i < end; i++)
            {
                int d = 0;
                for (int j = 0; j < DataLoader.Dim; j++)
                {
                    int diff = query[j] - dims[j][i];
                    d += diff * diff;
                }
                if (d < worstD)
                {
                    bestD[worst] = d;
                    bestId[worst] = i;
                    worst = 0; worstD = bestD[0];
                    for (int j = 1; j < 5; j++)
                        if (bestD[j] > worstD) { worst = j; worstD = bestD[j]; }
                }
            }

            scanned += sz;
            if (scanned >= budget) break;
        }

        int fraudCount = 0;
        for (int k = 0; k < 5; k++)
            if (bestD[k] != int.MaxValue) fraudCount += labels[bestId[k]];
        return fraudCount;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Acc(
        Vector256<short> q, ref sbyte dimBase, nuint off,
        ref Vector256<int> a, ref Vector256<int> b,
        ref Vector256<int> c, ref Vector256<int> d)
    {
        var packed = Vector256.LoadUnsafe(ref dimBase, off);
        var (vLo, vHi) = Vector256.Widen(packed);

        var diffLo = q - vLo;
        var diffHi = q - vHi;

        var (loLo, loHi) = Vector256.Widen(diffLo);
        var (hiLo, hiHi) = Vector256.Widen(diffHi);

        a += loLo * loLo;
        b += loHi * loHi;
        c += hiLo * hiLo;
        d += hiHi * hiHi;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool AllExceed(
        Vector256<int> a, Vector256<int> b, Vector256<int> c, Vector256<int> d,
        Vector256<int> w)
    {
        uint ma = Vector256.LessThan(a, w).ExtractMostSignificantBits();
        uint mb = Vector256.LessThan(b, w).ExtractMostSignificantBits();
        uint mc = Vector256.LessThan(c, w).ExtractMostSignificantBits();
        uint md = Vector256.LessThan(d, w).ExtractMostSignificantBits();
        return (ma | mb | mc | md) == 0;
    }
}
