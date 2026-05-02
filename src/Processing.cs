using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text.Json;
using Rinha2026.Models;

namespace Rinha2026.Services;

[StructLayout(LayoutKind.Sequential)]
public struct VpNode
{
    public int PivotIdx;    // -1 quando o nó é folha
    public long Radius;
    public double SqrtRadius;
    public int Left;
    public int Right;
    public int LeafStart;   // offset em LeafIndices
    public int LeafLen;     // > 0 quando folha; 0 quando interno
}

public unsafe class DataLoader : IDisposable
{
    public const int Scale = 32_767;
    public const int Dim = 14;
    public const int Stride = 16;       // 14 dims + 2 zero pad
    public const int K = 5;
    public const int VpNone = -1;
    public const int MaxStackCapacity = 256;

    public Dictionary<string, double> MccRisk { get; }
    public NormalizationConfig Normalization { get; }
    public byte[] Labels { get; }       // pequeno, fica em managed heap
    public int Count { get; }
    public int NodeCount { get; }
    public int IndicesCount { get; }

    // Os 3 maiores buffers ficam em memória nativa (NativeMemory.Alloc) pra não
    // contar contra o GC heap hard limit (~75% do cgroup). Sobra heap gerenciado
    // suficiente pra Kestrel/JSON sob carga sem cair em managed OOM.
    public short* Vectors { get; }       // Count * Stride
    public VpNode* Nodes { get; }        // NodeCount
    public int* LeafIndices { get; }     // IndicesCount

    public DataLoader(string resourcesPath)
    {
        MccRisk = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "mcc_risk.json")),
            AppJsonContext.Default.DictionaryStringDouble)!;

        Normalization = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(resourcesPath, "normalization.json")),
            AppJsonContext.Default.NormalizationConfig)!;

        // Formato v5: [int32 count][N*Stride shorts][N labels][int32 nodeCount][nodes][int32 indCount][indices]
        using var fs = File.OpenRead(Path.Combine(resourcesPath, "references.bin"));

        Span<byte> intBuf = stackalloc byte[4];

        fs.ReadExactly(intBuf);
        Count = BinaryPrimitives.ReadInt32LittleEndian(intBuf);

        long vecBytes = (long)Count * Stride * sizeof(short);
        Vectors = (short*)NativeMemory.AllocZeroed((nuint)vecBytes);
        ReadExact(fs, (byte*)Vectors, vecBytes);

        Labels = new byte[Count];
        fs.ReadExactly(Labels);

        fs.ReadExactly(intBuf);
        NodeCount = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
        long nodeBytes = (long)NodeCount * sizeof(VpNode);
        Nodes = (VpNode*)NativeMemory.AllocZeroed((nuint)nodeBytes);
        ReadExact(fs, (byte*)Nodes, nodeBytes);

        fs.ReadExactly(intBuf);
        IndicesCount = BinaryPrimitives.ReadInt32LittleEndian(intBuf);
        long indBytes = (long)IndicesCount * sizeof(int);
        LeafIndices = (int*)NativeMemory.AllocZeroed((nuint)indBytes);
        ReadExact(fs, (byte*)LeafIndices, indBytes);
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
        if (Vectors != null) NativeMemory.Free(Vectors);
        if (Nodes != null) NativeMemory.Free(Nodes);
        if (LeafIndices != null) NativeMemory.Free(LeafIndices);
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
    private readonly short* _vectors;
    private readonly VpNode* _nodes;
    private readonly int* _leafIndices;
    private readonly byte[] _labels;
    private readonly int _nodeCount;

    public static readonly byte[][] PrecomputedResponses = BuildResponses();

    public FraudDetector(DataLoader data)
    {
        Data = data;
        _vectors = data.Vectors;
        _nodes = data.Nodes;
        _leafIndices = data.LeafIndices;
        _labels = data.Labels;
        _nodeCount = data.NodeCount;
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
        Vectorizer.Vectorize(req, Data.Normalization, Data.MccRisk, query);
        return SearchKnn(query);
    }

    private static long _sNodesVisited;
    private static long _sLeafDists;
    private static long _sPivotDists;
    private static long _sPruned;
    private static long _sNotPruned;
    private static long _sSearchCalls;

    public int SearchKnn(ReadOnlySpan<short> query)
    {
        if (_nodeCount == 0) return 0;

        int k = DataLoader.K;
        var labels = _labels;
        VpNode* nodes = _nodes;
        int* leafIndices = _leafIndices;
        short* vectors = _vectors;

        Span<long> bestD = stackalloc long[5] { long.MaxValue, long.MaxValue, long.MaxValue, long.MaxValue, long.MaxValue };
        Span<int> bestId = stackalloc int[5];
        int found = 0;

        Span<int> stack = stackalloc int[DataLoader.MaxStackCapacity];
        int stackLen = 1;
        stack[0] = 0;

        var qVec = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(query));

        int nodesVisited = 0;
        int leafDists = 0;
        int pivotDists = 0;
        int pruned = 0;
        int notPruned = 0;

        while (stackLen > 0)
        {
            int nodeIdx = stack[--stackLen];
            VpNode* node = nodes + nodeIdx;
            nodesVisited++;

            if (node->LeafLen > 0)
            {
                int leafEnd = node->LeafStart + node->LeafLen;
                leafDists += node->LeafLen;
                for (int i = node->LeafStart; i < leafEnd; i++)
                {
                    int refIdx = leafIndices[i];
                    long dist = DistByQuery(qVec, vectors, refIdx);
                    InsertTopK(bestD, bestId, ref found, refIdx, dist, k);
                }
                continue;
            }

            int pivotIdx = node->PivotIdx;
            long pivotDist = DistByQuery(qVec, vectors, pivotIdx);
            pivotDists++;
            InsertTopK(bestD, bestId, ref found, pivotIdx, pivotDist, k);

            int near, far;
            if (pivotDist <= node->Radius) { near = node->Left;  far = node->Right; }
            else                            { near = node->Right; far = node->Left;  }

            bool canVisitFar = false;
            if (far != DataLoader.VpNone)
            {
                if (found < k) canVisitFar = true;
                else
                {
                    long worstDist = bestD[k - 1];
                    double pivotNorm = Math.Sqrt(pivotDist);
                    double radiusNorm = node->SqrtRadius;
                    double worstNorm = Math.Sqrt(worstDist);
                    canVisitFar = Math.Abs(pivotNorm - radiusNorm) <= worstNorm;
                }
                if (canVisitFar) notPruned++; else pruned++;
            }

            if (canVisitFar) stack[stackLen++] = far;
            if (near != DataLoader.VpNone) stack[stackLen++] = near;
        }

        Interlocked.Add(ref _sNodesVisited, nodesVisited);
        Interlocked.Add(ref _sLeafDists, leafDists);
        Interlocked.Add(ref _sPivotDists, pivotDists);
        Interlocked.Add(ref _sPruned, pruned);
        Interlocked.Add(ref _sNotPruned, notPruned);
        var n = Interlocked.Increment(ref _sSearchCalls);
        if (n % 100 == 0)
        {
            Console.WriteLine($"[SEARCH n={n}] nodes={Interlocked.Read(ref _sNodesVisited)/n} leafDists={Interlocked.Read(ref _sLeafDists)/n} pivotDists={Interlocked.Read(ref _sPivotDists)/n} pruned={Interlocked.Read(ref _sPruned)/n} notPruned={Interlocked.Read(ref _sNotPruned)/n} (avg/req)");
        }

        int fraudCount = 0;
        for (int i = 0; i < found; i++)
            fraudCount += labels[bestId[i]];
        return fraudCount;
    }

    // L2² entre query (Vector256<short>) e a ref no índice refIdx.
    // Com Scale=32767 (igual ao gabarito), diff cabe em int mas diff² estoura int.
    // Estratégia: widen short→int antes de subtrair, depois Avx2.Multiply (vpmuldq)
    // pra fazer i32×i32→i64 em SIMD, somando lanes ao final em long.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long DistByQuery(Vector256<short> qVec, short* vectors, int refIdx)
    {
        var v = Vector256.Load(vectors + refIdx * DataLoader.Stride);
        var (qLo, qHi) = Vector256.Widen(qVec);   // 8 ints + 8 ints, valor pleno preservado
        var (vLo, vHi) = Vector256.Widen(v);
        var dLo = qLo - vLo;                       // 8 ints, lanes em [-65534, 65534]
        var dHi = qHi - vHi;

        // Avx2.Multiply pega o low 32 de cada 64-bit lane e faz i32×i32→i64.
        // dLo tem 8 i32; visto como i64 são 4 lanes [d0|d1, d2|d3, d4|d5, d6|d7].
        // Multiply(dLo, dLo) → 4 i64: [d0², d2², d4², d6²]
        // Após shr32, low de cada 64-bit lane vira d1, d3, d5, d7 → [d1², d3², d5², d7²]
        var sqLoEven = Avx2.Multiply(dLo, dLo);
        var sqLoOdd  = Avx2.Multiply(
            Avx2.ShiftRightLogical(dLo.AsInt64(), 32).AsInt32(),
            Avx2.ShiftRightLogical(dLo.AsInt64(), 32).AsInt32());

        var sqHiEven = Avx2.Multiply(dHi, dHi);
        var sqHiOdd  = Avx2.Multiply(
            Avx2.ShiftRightLogical(dHi.AsInt64(), 32).AsInt32(),
            Avx2.ShiftRightLogical(dHi.AsInt64(), 32).AsInt32());

        var sum = (sqLoEven + sqLoOdd) + (sqHiEven + sqHiOdd);  // 4 i64 lanes
        return Vector256.Sum(sum);
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
