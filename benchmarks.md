# Benchmarks - Busca Vetorial IVF

Configuração: nprobe=10, 2048 clusters, k=5, NPROBE_RETRY_EXTRA=75

Medido sob carga com ~2 instâncias (api1/api2), ~1000+ requests cada.

## Baseline (escalar, model binding oculto)

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | não medido | — |
| Vetorização | ~3 µs | ~1% |
| Centroid ranking (2048 centróides float) | ~59 µs | ~21% |
| Cluster scan (~6900 distComps) | ~200 µs | ~72% |
| Response write | ~13 µs | ~5% |
| **Total medido** | **~280 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~125 µs | +45% extra |

## Com SIMD AVX2 no cluster scan

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | não medido | — |
| Vetorização | ~3 µs | ~2% |
| Centroid ranking (2048 centróides float) | ~59 µs | ~48% |
| Cluster scan (~6900 distComps) | ~47 µs | ~38% |
| Response write | ~11 µs | ~9% |
| **Total medido** | **~124 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~120 µs | +97% extra |

Speedup scan: **4.3x** (200µs → 47µs)

## Com SIMD AVX2 no cluster scan + centroid ranking

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | não medido | — |
| Vetorização | ~3 µs | ~3% |
| Centroid ranking (2048 centróides float) | ~20 µs | ~22% |
| Cluster scan (~6900 distComps) | ~51 µs | ~56% |
| Response write | ~12 µs | ~13% |
| **Total medido** | **~90 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~90 µs | +100% extra |

Speedup centroid: **3x** (59µs → 20µs)

## Com SIMD + bypass routing/model binding (desserialização visível)

Nprobe=8, bitset no retry.

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (JsonSerializer stream) | ~55 µs | ~40% |
| Vetorização | ~3 µs | ~2% |
| Centroid ranking (2048 centróides float) | ~19 µs | ~14% |
| Cluster scan (~6300 distComps) | ~43 µs | ~31% |
| Response write | ~12 µs | ~9% |
| **Total request (real)** | **~138 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~70 µs | +51% extra |

Nota: as medições anteriores não incluíam a desserialização (~55µs ocultos no model binding).
O request real sempre custou ~138µs — agora visível. Próximo alvo: desserialização.

## Com Utf8JsonReader manual (zero-alloc parsing)

Nprobe=8, bitset no retry. Parser manual com Utf8JsonReader + struct flat no stack.
Elimina 6 records, string[], strings de datas. Única alocação: string do MCC.

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (Utf8JsonReader manual) | ~20 µs | ~21% |
| Vetorização | ~2 µs | ~2% |
| Centroid ranking (2048 centróides float) | ~19 µs | ~20% |
| Cluster scan (~6250 distComps) | ~42 µs | ~43% |
| Response write | ~10 µs | ~10% |
| **Total request** | **~97 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~60 µs | +62% extra |

Speedup desserialização: **2.75x** (55µs → 20µs)

## Resumo de evolução

| Etapa | Total medido | Total real estimado |
|-------|-------------|---------------------|
| Baseline escalar | 280 µs | ~335 µs |
| +SIMD scan | 124 µs | ~179 µs |
| +SIMD centroid | 90 µs | ~145 µs |
| +bypass framework + bitset | 138 µs | **138 µs** (agora real) |
| +Utf8JsonReader manual | 97 µs | **97 µs** |

O speedup real (ponta-a-ponta) é **~3.5x** (335µs → 97µs).
