# Benchmarks - Busca Vetorial IVF

Configuração: nprobe=10, 2048 clusters, k=5, NPROBE_RETRY_EXTRA=75

Medido sob carga com ~2 instâncias (api1/api2), ~1000+ requests cada.

| Fase | Tempo médio | % do request |
|------|-------------|--------------|
| Desserialização (model binding) | ~0 µs | ~0% |
| Vetorização | ~3 µs | ~1% |
| Centroid ranking (2048 centróides float) | ~59 µs | ~21% |
| Cluster scan (~6900 distComps) | ~200 µs | ~72% |
| Response write | ~13 µs | ~5% |
| **Total request** | **~280 µs** | **100%** |
| Retry limítrofe (score 0.4/0.6) | ~125 µs | +45% extra |

- Retry acontece em ~3% dos requests.
- O gargalo é o cluster scan (comparações de distância vetor a vetor).
- O centroid ranking é o segundo custo mais relevante.
