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
    private readonly HnswIndex _hnsw;
    public static readonly byte[][] PrecomputedResponses = BuildResponses();

    public FraudDetector(DataLoader data)
    {
        _data = data;
        _hnsw = new HnswIndex(data.References, data.Count);
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
        return CountFraudNeighbors(query, 5);
    }

    private int CountFraudNeighbors(Span<short> query, int k)
    {
        Span<int> ids = stackalloc int[5];
        Span<int> dists = stackalloc int[5];
        int n = _hnsw.SearchKnn(query, ids, dists, k);
        var labels = _data.Labels;
        int fraudCount = 0;
        for (int i = 0; i < n; i++) fraudCount += labels[ids[i]];
        return fraudCount;
    }
}

// Hierarchical Navigable Small World (HNSW) index over the int16 reference vectors.
// Build is single-threaded at startup. Search is allocation-free and thread-safe.
public sealed class HnswIndex
{
    private const int M = 16;
    private const int M0 = 32;
    private const int EfConstruction = 100;
    public const int EfSearch = 64;
    private static readonly double Ml = 1.0 / Math.Log(M);

    private readonly short[] _vectors;
    private readonly int _count;
    private readonly int[] _level0;          // _count * M0 neighbour ids, packed by node
    private readonly byte[] _level0Cnt;      // _count
    private readonly Dictionary<int, int[]>[] _upper; // [layer-1][nodeId] = neighbour ids
    private readonly int _entryPoint;
    private readonly int _topLayer;

    [ThreadStatic] private static int[]? _visitedTls;
    [ThreadStatic] private static int _genTls;

    public int Count => _count;

    public HnswIndex(short[] vectors, int count, int seed = 42)
    {
        _vectors = vectors;
        _count = count;
        _level0 = new int[count * M0];
        _level0Cnt = new byte[count];
        _upper = Array.Empty<Dictionary<int, int[]>>();

        if (count == 0) return;

        var rng = new Random(seed);
        var l0 = new List<(int dist, int id)>[count];
        for (int i = 0; i < count; i++) l0[i] = new List<(int, int)>(M0 + 1);
        var upper = new List<Dictionary<int, List<(int dist, int id)>>>();

        int topLayer = SampleLayer(rng);
        int ep = 0;
        for (int l = 1; l <= topLayer; l++)
        {
            while (upper.Count < l) upper.Add(new Dictionary<int, List<(int, int)>>());
            upper[l - 1][0] = new List<(int, int)>(M + 1);
        }

        var visited = new int[count];
        int gen = 0;

        for (int q = 1; q < count; q++)
        {
            int qLayer = SampleLayer(rng);
            for (int l = 1; l <= qLayer; l++)
            {
                while (upper.Count < l) upper.Add(new Dictionary<int, List<(int, int)>>());
                upper[l - 1][q] = new List<(int, int)>(M + 1);
            }

            int currNear = ep;
            int currD = DistByIds(q, ep);

            // Greedy descent from top to qLayer+1.
            for (int level = topLayer; level > qLayer; level--)
            {
                bool changed = true;
                while (changed)
                {
                    changed = false;
                    if (upper[level - 1].TryGetValue(currNear, out var nb))
                    {
                        for (int j = 0; j < nb.Count; j++)
                        {
                            int d = DistByIds(q, nb[j].id);
                            if (d < currD) { currD = d; currNear = nb[j].id; changed = true; }
                        }
                    }
                }
            }

            // ef-search and bidirectional linking from min(top, qLayer) down to 0.
            int startLevel = Math.Min(topLayer, qLayer);
            for (int level = startLevel; level >= 0; level--)
            {
                var W = SearchLayerBuild(q, currNear, currD, level, EfConstruction, l0, upper, visited, ref gen);
                int Mmax = level == 0 ? M0 : M;
                int linkCount = Math.Min(M, W.Count);

                for (int li = 0; li < linkCount; li++)
                {
                    var (nbD, nbId) = W[li];
                    AddLink(q, nbId, nbD, level, l0, upper);
                    AddLink(nbId, q, nbD, level, l0, upper);
                    PruneIfNeeded(nbId, level, Mmax, l0, upper);
                }

                if (W.Count > 0) { currNear = W[0].id; currD = W[0].dist; }
            }

            if (qLayer > topLayer) { topLayer = qLayer; ep = q; }
        }

        _entryPoint = ep;
        _topLayer = topLayer;

        // Materialise level 0 to flat array.
        for (int i = 0; i < count; i++)
        {
            var list = l0[i];
            int n = Math.Min(list.Count, M0);
            _level0Cnt[i] = (byte)n;
            int baseIdx = i * M0;
            for (int j = 0; j < n; j++) _level0[baseIdx + j] = list[j].id;
        }

        // Materialise upper layers as id-only arrays.
        _upper = new Dictionary<int, int[]>[upper.Count];
        for (int l = 0; l < upper.Count; l++)
        {
            var dict = new Dictionary<int, int[]>(upper[l].Count);
            foreach (var kv in upper[l])
            {
                var src = kv.Value;
                var arr = new int[src.Count];
                for (int j = 0; j < arr.Length; j++) arr[j] = src[j].id;
                dict[kv.Key] = arr;
            }
            _upper[l] = dict;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SampleLayer(Random rng)
    {
        return (int)Math.Floor(-Math.Log(1.0 - rng.NextDouble()) * Ml);
    }

    private List<(int dist, int id)> SearchLayerBuild(
        int q, int entry, int entryD, int level, int ef,
        List<(int dist, int id)>[] l0,
        List<Dictionary<int, List<(int dist, int id)>>> upper,
        int[] visited, ref int gen)
    {
        gen++;
        if (gen == 0) { Array.Clear(visited); gen = 1; }
        visited[entry] = gen;

        // Pack (dist << 32) | (uint)id so the long ordering is the dist ordering.
        var cand = new PriorityQueue<int, long>();
        var res = new PriorityQueue<int, long>(); // negated key → behaves as max-heap
        cand.Enqueue(entry, ((long)entryD << 32) | (uint)entry);
        res.Enqueue(entry, -(((long)entryD << 32) | (uint)entry));
        int worstRes = entryD;

        while (cand.Count > 0)
        {
            cand.TryDequeue(out int cId, out long cKey);
            int cD = (int)(cKey >> 32);
            if (cD > worstRes && res.Count >= ef) break;

            if (level == 0)
            {
                var nb = l0[cId];
                for (int i = 0; i < nb.Count; i++)
                    Visit(q, nb[i].id, level, cand, res, visited, gen, ref worstRes, ef);
            }
            else if (upper[level - 1].TryGetValue(cId, out var nb))
            {
                for (int i = 0; i < nb.Count; i++)
                    Visit(q, nb[i].id, level, cand, res, visited, gen, ref worstRes, ef);
            }
        }

        var output = new List<(int, int)>(res.Count);
        while (res.Count > 0)
        {
            res.TryDequeue(out int id, out long negKey);
            long key = -negKey;
            output.Add(((int)(key >> 32), id));
        }
        output.Sort((a, b) => a.Item1.CompareTo(b.Item1));
        return output;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Visit(int q, int nbId, int level,
        PriorityQueue<int, long> cand, PriorityQueue<int, long> res,
        int[] visited, int gen, ref int worstRes, int ef)
    {
        if (visited[nbId] == gen) return;
        visited[nbId] = gen;
        int d = DistByIds(q, nbId);
        if (d < worstRes || res.Count < ef)
        {
            long key = ((long)d << 32) | (uint)nbId;
            cand.Enqueue(nbId, key);
            res.Enqueue(nbId, -key);
            if (res.Count > ef) res.Dequeue();
            res.TryPeek(out _, out long topNeg);
            worstRes = (int)((-topNeg) >> 32);
        }
    }

    private static void AddLink(int from, int to, int dist, int level,
        List<(int, int)>[] l0,
        List<Dictionary<int, List<(int, int)>>> upper)
    {
        if (level == 0) l0[from].Add((dist, to));
        else upper[level - 1][from].Add((dist, to));
    }

    private static void PruneIfNeeded(int nodeId, int level, int Mmax,
        List<(int dist, int id)>[] l0,
        List<Dictionary<int, List<(int dist, int id)>>> upper)
    {
        var list = level == 0 ? l0[nodeId] : upper[level - 1][nodeId];
        if (list.Count <= Mmax) return;
        list.Sort((a, b) => a.dist.CompareTo(b.dist));
        list.RemoveRange(Mmax, list.Count - Mmax);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int DistByIds(int a, int b)
    {
        ref var refBase = ref MemoryMarshal.GetArrayDataReference(_vectors);
        var aVec = Vector256.LoadUnsafe(ref refBase, (nuint)(a * DataLoader.Stride));
        var bVec = Vector256.LoadUnsafe(ref refBase, (nuint)(b * DataLoader.Stride));
        var diff = aVec - bVec;
        var (lo, hi) = Vector256.Widen(diff);
        var sq = lo * lo + hi * hi;
        return Vector256.Sum(sq);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int DistByQuery(Vector256<short> q, int b)
    {
        ref var refBase = ref MemoryMarshal.GetArrayDataReference(_vectors);
        var bVec = Vector256.LoadUnsafe(ref refBase, (nuint)(b * DataLoader.Stride));
        var diff = q - bVec;
        var (lo, hi) = Vector256.Widen(diff);
        var sq = lo * lo + hi * hi;
        return Vector256.Sum(sq);
    }

    public int SearchKnn(ReadOnlySpan<short> query, Span<int> outIds, Span<int> outDists, int k, int ef = EfSearch)
    {
        if (_count == 0) return 0;

        var qVec = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(query));

        var visited = _visitedTls;
        if (visited is null || visited.Length < _count)
        {
            visited = new int[_count];
            _visitedTls = visited;
        }
        int gen = ++_genTls;
        if (gen == 0) { Array.Clear(visited); _genTls = gen = 1; }

        int currNear = _entryPoint;
        int currD = DistByQuery(qVec, currNear);

        // Greedy descent from top layer to layer 1.
        for (int level = _topLayer; level > 0; level--)
        {
            bool changed = true;
            while (changed)
            {
                changed = false;
                if (_upper[level - 1].TryGetValue(currNear, out var nb))
                {
                    for (int j = 0; j < nb.Length; j++)
                    {
                        int d = DistByQuery(qVec, nb[j]);
                        if (d < currD) { currD = d; currNear = nb[j]; changed = true; }
                    }
                }
            }
        }

        // ef-search at level 0 with stack-allocated 1-indexed binary heaps.
        Span<long> cand = stackalloc long[ef + 1];
        Span<long> res = stackalloc long[ef + 1];
        int candN = 0, resN = 0;

        long entryKey = ((long)currD << 32) | (uint)currNear;
        cand[++candN] = entryKey;
        res[++resN] = entryKey;
        int worstRes = currD;
        visited[currNear] = gen;

        while (candN > 0)
        {
            long top = cand[1];
            cand[1] = cand[candN--];
            if (candN > 0) HeapDownMin(cand, candN, 1);
            int cD = (int)(top >> 32);
            int cId = (int)(top & 0xFFFFFFFFL);

            if (cD > worstRes && resN >= ef) break;

            int baseIdx = cId * M0;
            int cnt = _level0Cnt[cId];
            for (int j = 0; j < cnt; j++)
            {
                int nbId = _level0[baseIdx + j];
                if (visited[nbId] == gen) continue;
                visited[nbId] = gen;
                int nbD = DistByQuery(qVec, nbId);
                if (nbD < worstRes || resN < ef)
                {
                    long nbKey = ((long)nbD << 32) | (uint)nbId;
                    cand[++candN] = nbKey;
                    HeapUpMin(cand, candN);
                    if (resN < ef)
                    {
                        res[++resN] = nbKey;
                        HeapUpMax(res, resN);
                    }
                    else if (nbD < worstRes)
                    {
                        res[1] = nbKey;
                        HeapDownMax(res, resN, 1);
                    }
                    worstRes = (int)(res[1] >> 32);
                }
            }
        }

        // Extract top-k by sorting the result heap by ascending dist.
        Span<long> sorted = stackalloc long[resN];
        for (int i = 0; i < resN; i++) sorted[i] = res[i + 1];
        sorted.Sort();
        int outN = Math.Min(k, resN);
        for (int i = 0; i < outN; i++)
        {
            outIds[i] = (int)(sorted[i] & 0xFFFFFFFFL);
            outDists[i] = (int)(sorted[i] >> 32);
        }
        return outN;
    }

    private static void HeapUpMin(Span<long> h, int i)
    {
        while (i > 1)
        {
            int p = i >> 1;
            if (h[p] <= h[i]) break;
            (h[p], h[i]) = (h[i], h[p]);
            i = p;
        }
    }

    private static void HeapDownMin(Span<long> h, int n, int i)
    {
        while (true)
        {
            int l = i << 1, r = l + 1, m = i;
            if (l <= n && h[l] < h[m]) m = l;
            if (r <= n && h[r] < h[m]) m = r;
            if (m == i) break;
            (h[m], h[i]) = (h[i], h[m]);
            i = m;
        }
    }

    private static void HeapUpMax(Span<long> h, int i)
    {
        while (i > 1)
        {
            int p = i >> 1;
            if (h[p] >= h[i]) break;
            (h[p], h[i]) = (h[i], h[p]);
            i = p;
        }
    }

    private static void HeapDownMax(Span<long> h, int n, int i)
    {
        while (true)
        {
            int l = i << 1, r = l + 1, m = i;
            if (l <= n && h[l] > h[m]) m = l;
            if (r <= n && h[r] > h[m]) m = r;
            if (m == i) break;
            (h[m], h[i]) = (h[i], h[m]);
            i = m;
        }
    }
}
