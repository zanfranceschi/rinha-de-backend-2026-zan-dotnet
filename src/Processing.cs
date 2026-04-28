using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

public class DataLoader
{
    public const int Scale = 127;     // sbyte range
    public const int Dim = 14;

    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public sbyte[][] Dims { get; }   // [Dim][Count] — SoA, sbyte
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

        // Formato v3 (SoA, sbyte):
        //   [int32 count]
        //   [14 × count sbytes — coluna por coluna]
        //   [count bytes labels]
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
        return CountFraudNeighborsLinear(query);
    }

    private int CountFraudNeighborsLinear(ReadOnlySpan<sbyte> query)
    {
        var dims = _data.Dims;
        int count = _data.Count;
        var labels = _data.Labels;

        // Broadcast da query como Vector256<short> (a aritmética é feita em short
        // pra evitar overflow de sbyte na subtração).
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

        // Top-5 (sem alocações).
        Span<int> bestD = stackalloc int[5] { int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue };
        Span<int> bestId = stackalloc int[5];
        int worst = 0;
        int worstD = int.MaxValue;

        // Buffer pra extrair as 32 distâncias por iteração.
        Span<int> dists = stackalloc int[32];
        ref var distsRef = ref MemoryMarshal.GetReference(dists);

        int simdEnd = count - (count & 31); // múltiplo de 32 (refs por iter)
        int i = 0;
        for (; i < simdEnd; i += 32)
        {
            nuint off = (nuint)i;
            var aA = Vector256<int>.Zero;
            var aB = Vector256<int>.Zero;
            var aC = Vector256<int>.Zero;
            var aD = Vector256<int>.Zero;
            var wVec = Vector256.Create(worstD);

            // Ordem das dims pra apertar worstD rápido (igual passo 3).
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

            // Mapeamento ref → posição em `dists`:
            //   aA → refs i+0 .. i+7    (lower-lower)
            //   aB → refs i+8 .. i+15   (lower-upper)
            //   aC → refs i+16 .. i+23  (upper-lower)
            //   aD → refs i+24 .. i+31  (upper-upper)
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

        // Cauda escalar (resto que não fecha 32).
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

    // Acumula a contribuição de uma dim em 4 acc's de int (8 lanes cada → 32 refs total).
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Acc(
        Vector256<short> q, ref sbyte dimBase, nuint off,
        ref Vector256<int> a, ref Vector256<int> b,
        ref Vector256<int> c, ref Vector256<int> d)
    {
        var packed = Vector256.LoadUnsafe(ref dimBase, off);   // 32 sbytes
        var (vLo, vHi) = Vector256.Widen(packed);              // 16 shorts × 2

        var diffLo = q - vLo;                                  // 16 shorts (refs 0..15)
        var diffHi = q - vHi;                                  // 16 shorts (refs 16..31)

        var (loLo, loHi) = Vector256.Widen(diffLo);            // 8 ints × 2 (refs 0..7, 8..15)
        var (hiLo, hiHi) = Vector256.Widen(diffHi);            // 8 ints × 2 (refs 16..23, 24..31)

        a += loLo * loLo;
        b += loHi * loHi;
        c += hiLo * hiLo;
        d += hiHi * hiHi;
    }

    // True quando TODAS as 32 lanes têm acc >= worstD (nenhuma pode entrar no top-5).
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
