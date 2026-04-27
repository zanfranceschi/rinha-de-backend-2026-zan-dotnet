using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

public class DataLoader
{
    public const int Scale = 8192;
    public const int Dim = 14;
    private const int SourceStride = 16; // formato do .bin: 14 dims + 2 lanes de pad

    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public short[][] Dims { get; }   // [Dim][Count] — SoA
    public byte[] Labels { get; }
    public int Count { get; }

    public DataLoader(string resourcesPath)
    {
        MccRisk = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "mcc_risk.json")),
            AppJsonContext.Default.DictionaryStringDouble)!;

        Normalization = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "normalization.json")),
            AppJsonContext.Default.NormalizationConfig)!;

        using var br = new BinaryReader(File.OpenRead(Path.Combine(resourcesPath, "references.bin")));
        Count = br.ReadInt32();

        Dims = new short[Dim][];
        for (int j = 0; j < Dim; j++)
            Dims[j] = GC.AllocateUninitializedArray<short>(Count, pinned: true);

        // Lê em chunks e transpõe AoS -> SoA, descartando as 2 lanes de pad.
        const int ChunkRefs = 4096;
        var buf = new short[ChunkRefs * SourceStride];
        var bufBytes = MemoryMarshal.AsBytes(buf.AsSpan());
        int done = 0;
        while (done < Count)
        {
            int toRead = Math.Min(ChunkRefs, Count - done);
            int byteCount = toRead * SourceStride * sizeof(short);
            int got = br.Read(bufBytes.Slice(0, byteCount));
            if (got != byteCount) throw new InvalidDataException("references.bin truncado");

            for (int j = 0; j < Dim; j++)
            {
                var dst = Dims[j].AsSpan(done, toRead);
                for (int i = 0; i < toRead; i++)
                    dst[i] = buf[i * SourceStride + j];
            }
            done += toRead;
        }

        Labels = br.ReadBytes(Count);
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
        output[9] = req.Terminal.IsOnline ? (short)DataLoader.Scale : (short)0;
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
        Span<short> query = stackalloc short[DataLoader.Dim];
        Vectorizer.Vectorize(req, _data.Normalization, _data.MccRisk, query);
        return CountFraudNeighborsLinear(query);
    }

    private int CountFraudNeighborsLinear(ReadOnlySpan<short> query)
    {
        var dims = _data.Dims;
        int count = _data.Count;
        var labels = _data.Labels;

        // Broadcast da query: cada lane do Vector256<short> = query[j].
        var q0  = Vector256.Create(query[0]);
        var q1  = Vector256.Create(query[1]);
        var q2  = Vector256.Create(query[2]);
        var q3  = Vector256.Create(query[3]);
        var q4  = Vector256.Create(query[4]);
        var q5  = Vector256.Create(query[5]);
        var q6  = Vector256.Create(query[6]);
        var q7  = Vector256.Create(query[7]);
        var q8  = Vector256.Create(query[8]);
        var q9  = Vector256.Create(query[9]);
        var q10 = Vector256.Create(query[10]);
        var q11 = Vector256.Create(query[11]);
        var q12 = Vector256.Create(query[12]);
        var q13 = Vector256.Create(query[13]);

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

        // Top-5 (sem alocações).
        Span<int> bestD = stackalloc int[5] { int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue };
        Span<int> bestId = stackalloc int[5];
        int worst = 0;
        int worstD = int.MaxValue;

        Span<int> dists = stackalloc int[16];
        ref var distsRef = ref MemoryMarshal.GetReference(dists);

        int simdEnd = count - (count & 15); // múltiplo de 16
        int i = 0;
        for (; i < simdEnd; i += 16)
        {
            nuint off = (nuint)i;
            var acc0 = Vector256<int>.Zero;
            var acc1 = Vector256<int>.Zero;

            // 14 dims; widen short->int produz (lower 8 lanes, upper 8 lanes).
            // diff² acumulado em dois acc's int.
            Accumulate(q0,  ref d0,  off, ref acc0, ref acc1);
            Accumulate(q1,  ref d1,  off, ref acc0, ref acc1);
            Accumulate(q2,  ref d2,  off, ref acc0, ref acc1);
            Accumulate(q3,  ref d3,  off, ref acc0, ref acc1);
            Accumulate(q4,  ref d4,  off, ref acc0, ref acc1);
            Accumulate(q5,  ref d5,  off, ref acc0, ref acc1);
            Accumulate(q6,  ref d6,  off, ref acc0, ref acc1);
            Accumulate(q7,  ref d7,  off, ref acc0, ref acc1);
            Accumulate(q8,  ref d8,  off, ref acc0, ref acc1);
            Accumulate(q9,  ref d9,  off, ref acc0, ref acc1);
            Accumulate(q10, ref d10, off, ref acc0, ref acc1);
            Accumulate(q11, ref d11, off, ref acc0, ref acc1);
            Accumulate(q12, ref d12, off, ref acc0, ref acc1);
            Accumulate(q13, ref d13, off, ref acc0, ref acc1);

            acc0.StoreUnsafe(ref distsRef);
            acc1.StoreUnsafe(ref distsRef, 8);

            for (int k = 0; k < 16; k++)
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

        // Cauda escalar (resto que não fecha 16).
        for (; i < count; i++)
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

        int fraudCount = 0;
        for (int k = 0; k < 5; k++)
            if (bestD[k] != int.MaxValue) fraudCount += labels[bestId[k]];
        return fraudCount;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Accumulate(
        Vector256<short> q, ref short dimBase, nuint off,
        ref Vector256<int> acc0, ref Vector256<int> acc1)
    {
        var v = Vector256.LoadUnsafe(ref dimBase, off);
        var diff = q - v;
        var (lo, hi) = Vector256.Widen(diff);
        acc0 += lo * lo;
        acc1 += hi * hi;
    }
}
