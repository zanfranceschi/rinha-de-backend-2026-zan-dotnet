# Benchmarks - Busca Vetorial IVF

Configuração: nprobe=10, 2048 clusters, k=5, NPROBE_RETRY_EXTRA=75

Medido sob carga com ~2 instâncias (api1/api2), ~1000+ requests cada.

## Baseline (escalar)

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | ~0 µs | ~0% |
| Vetorização | ~3 µs | ~1% |
| Centroid ranking (2048 centróides float) | ~59 µs | ~21% |
| Cluster scan (~6900 distComps) | ~200 µs | ~72% |
| Response write | ~13 µs | ~5% |
| **Total request** | **~280 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~125 µs | +45% extra |

## Com SIMD AVX2 no cluster scan

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | ~0 µs | ~0% |
| Vetorização | ~3 µs | ~2% |
| Centroid ranking (2048 centróides float) | ~59 µs | ~48% |
| Cluster scan (~6900 distComps) | ~47 µs | ~38% |
| Response write | ~11 µs | ~9% |
| **Total request** | **~124 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~120 µs | +97% extra |

Speedup scan: **4.3x** (200µs → 47µs)
Speedup total: **2.3x** (280µs → 124µs)

## Com SIMD AVX2 no cluster scan + centroid ranking

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | ~0 µs | ~0% |
| Vetorização | ~3 µs | ~3% |
| Centroid ranking (2048 centróides float) | ~20 µs | ~22% |
| Cluster scan (~6900 distComps) | ~51 µs | ~56% |
| Response write | ~12 µs | ~13% |
| **Total request** | **~90 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~90 µs | +100% extra |

Speedup centroid: **3x** (59µs → 20µs)
Speedup total vs baseline: **3.1x** (280µs → 90µs)
