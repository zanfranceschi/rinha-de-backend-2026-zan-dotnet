using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

public class DataLoader
{
    public const int Scale = 8192;
    public const int Stride = 16; // 14 dims + 2 pad

    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public short[] References { get; } // flat: count * Stride shorts
    public byte[] Labels { get; }       // 1 = fraud, 0 = legit
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
        References = GC.AllocateUninitializedArray<short>(Count * Stride, pinned: true);
        var refBytes = MemoryMarshal.AsBytes(References.AsSpan());
        br.Read(refBytes);
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

    // Fixed format: "YYYY-MM-DDTHH:MM:SSZ" (20 chars). No allocs, no culture lookup.
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
        // output is at least 16 shorts; lanes 14,15 left as zero pad (caller pre-zeros).
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
        Span<short> query = stackalloc short[DataLoader.Stride];
        Vectorizer.Vectorize(req, _data.Normalization, _data.MccRisk, query);
        return CountFraudNeighborsLinear(query);
    }

    private int CountFraudNeighborsLinear(ReadOnlySpan<short> query)
    {
        var qVec = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(query));
        ref var refBase = ref MemoryMarshal.GetArrayDataReference(_data.References);
        int count = _data.Count;

        // Top-5: distâncias e labels; worst no índice 'worst'.
        Span<int> bestD = stackalloc int[5] { int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue };
        Span<int> bestId = stackalloc int[5];
        int worst = 0;
        int worstD = int.MaxValue;

        for (int i = 0; i < count; i++)
        {
            var v = Vector256.LoadUnsafe(ref refBase, (nuint)(i * DataLoader.Stride));
            var diff = qVec - v;
            var (lo, hi) = Vector256.Widen(diff);
            int d = Vector256.Sum(lo * lo + hi * hi);

            if (d < worstD)
            {
                bestD[worst] = d;
                bestId[worst] = i;
                worst = 0;
                worstD = bestD[0];
                for (int j = 1; j < 5; j++)
                    if (bestD[j] > worstD) { worst = j; worstD = bestD[j]; }
            }
        }

        var labels = _data.Labels;
        int fraudCount = 0;
        for (int i = 0; i < 5; i++)
            if (bestD[i] != int.MaxValue) fraudCount += labels[bestId[i]];
        return fraudCount;
    }
}

