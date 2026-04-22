using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

public class DataLoader
{
    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public List<Reference> References { get; }

    public DataLoader(string resourcesPath)
    {
        MccRisk = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "mcc_risk.json")),
            AppJsonContext.Default.DictionaryStringDouble)!;

        Normalization = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "normalization.json")),
            AppJsonContext.Default.NormalizationConfig)!;

        using var br = new BinaryReader(File.OpenRead(Path.Combine(resourcesPath, "references.bin")));
        var count = br.ReadInt32();
        References = new List<Reference>(count);
        for (int i = 0; i < count; i++)
        {
            var vector = new double[14];
            for (int d = 0; d < 14; d++)
                vector[d] = br.ReadDouble();
            var label = br.ReadByte() == 1 ? "fraud" : "legit";
            References.Add(new Reference(vector, label));
        }
    }
}

public static class Vectorizer
{
    public static double[] Vectorize(FraudRequest req, NormalizationConfig norm, Dictionary<string, double> mccRisk)
    {
        var vector = new double[14];

        var requestedAt = DateTime.Parse(req.Transaction.RequestedAt).ToUniversalTime();
        var dow = ((int)requestedAt.DayOfWeek + 6) % 7;

        vector[0] = Clamp(req.Transaction.Amount / norm.MaxAmount);
        vector[1] = Clamp(req.Transaction.Installments / norm.MaxInstallments);
        vector[2] = Clamp((req.Transaction.Amount / req.Customer.AvgAmount) / norm.AmountVsAvgRatio);
        vector[3] = requestedAt.Hour / 23.0;
        vector[4] = dow / 6.0;

        if (req.LastTransaction is not null)
        {
            var lastTs = DateTime.Parse(req.LastTransaction.Timestamp).ToUniversalTime();
            var minutes = (requestedAt - lastTs).TotalMinutes;
            vector[5] = Clamp(minutes / norm.MaxMinutes);
            vector[6] = Clamp(req.LastTransaction.KmFromCurrent / norm.MaxKm);
        }
        else
        {
            vector[5] = -1;
            vector[6] = -1;
        }

        vector[7] = Clamp(req.Terminal.KmFromHome / norm.MaxKm);
        vector[8] = Clamp(req.Customer.TxCount24h / norm.MaxTxCount24h);
        vector[9] = req.Terminal.IsOnline ? 1 : 0;
        vector[10] = req.Terminal.CardPresent ? 1 : 0;
        vector[11] = req.Customer.KnownMerchants.Contains(req.Merchant.Id) ? 0 : 1;
        vector[12] = mccRisk.GetValueOrDefault(req.Merchant.Mcc, 0.5);
        vector[13] = Clamp(req.Merchant.AvgAmount / norm.MaxMerchantAvgAmount);

        return vector;
    }

    private static double Clamp(double x) => Math.Clamp(x, 0.0, 1.0);
}

public class FraudDetector
{
    private readonly DataLoader _data;

    public FraudDetector(DataLoader data)
    {
        _data = data;
    }

    public FraudResponse Evaluate(FraudRequest req)
    {
        var vector = Vectorizer.Vectorize(req, _data.Normalization, _data.MccRisk);
        var neighbors = FindNearest(vector, 5);
        var fraudCount = neighbors.Count(r => r.Label == "fraud");
        var score = (double)fraudCount / neighbors.Count;
        return new FraudResponse(score < 0.6, score);
    }

    private List<Reference> FindNearest(double[] query, int k)
    {
        var topDists = new double[k];
        var topRefs = new Reference[k];
        Array.Fill(topDists, double.MaxValue);

        foreach (var r in _data.References)
        {
            var dist = EuclideanDistance(query, r.Vector);
            if (dist >= topDists[k - 1])
                continue;

            int i = k - 2;
            while (i >= 0 && topDists[i] > dist)
            {
                topDists[i + 1] = topDists[i];
                topRefs[i + 1] = topRefs[i];
                i--;
            }
            topDists[i + 1] = dist;
            topRefs[i + 1] = r;
        }

        return new List<Reference>(topRefs);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double EuclideanDistance(double[] a, double[] b)
    {
        // 14 doubles: 3x Vector256<double> (4 each = 12) + 2 scalar
        ref var ra = ref a[0];
        ref var rb = ref b[0];

        var v0 = Vector256.LoadUnsafe(ref ra, 0) - Vector256.LoadUnsafe(ref rb, 0);
        var v1 = Vector256.LoadUnsafe(ref ra, 4) - Vector256.LoadUnsafe(ref rb, 4);
        var v2 = Vector256.LoadUnsafe(ref ra, 8) - Vector256.LoadUnsafe(ref rb, 8);

        var sum = v0 * v0 + v1 * v1 + v2 * v2;
        var result = Vector256.Sum(sum);

        var d12 = a[12] - b[12];
        var d13 = a[13] - b[13];
        return result + d12 * d12 + d13 * d13;
    }
}
