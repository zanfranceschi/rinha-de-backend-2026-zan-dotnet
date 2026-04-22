using System.Text.Json.Serialization;

namespace Rinha2026.Models;

public record Transaction(
    double Amount,
    int Installments,
    [property: JsonPropertyName("requested_at")] string RequestedAt);

public record Customer(
    [property: JsonPropertyName("avg_amount")] double AvgAmount,
    [property: JsonPropertyName("tx_count_24h")] int TxCount24h,
    [property: JsonPropertyName("known_merchants")] string[] KnownMerchants);

public record Merchant(
    string Id,
    string Mcc,
    [property: JsonPropertyName("avg_amount")] double AvgAmount);

public record Terminal(
    [property: JsonPropertyName("is_online")] bool IsOnline,
    [property: JsonPropertyName("card_present")] bool CardPresent,
    [property: JsonPropertyName("km_from_home")] double KmFromHome);

public record LastTransaction(
    string Timestamp,
    [property: JsonPropertyName("km_from_current")] double KmFromCurrent);

public record FraudRequest(
    string Id,
    Transaction Transaction,
    Customer Customer,
    Merchant Merchant,
    Terminal Terminal,
    [property: JsonPropertyName("last_transaction")] LastTransaction? LastTransaction);

public record FraudResponse(
    bool Approved,
    [property: JsonPropertyName("fraud_score")] double FraudScore);

public record Reference(
    [property: JsonPropertyName("vector")] double[] Vector,
    [property: JsonPropertyName("label")] string Label);

public record NormalizationConfig(
    [property: JsonPropertyName("max_amount")] double MaxAmount,
    [property: JsonPropertyName("max_installments")] double MaxInstallments,
    [property: JsonPropertyName("amount_vs_avg_ratio")] double AmountVsAvgRatio,
    [property: JsonPropertyName("max_minutes")] double MaxMinutes,
    [property: JsonPropertyName("max_km")] double MaxKm,
    [property: JsonPropertyName("max_tx_count_24h")] double MaxTxCount24h,
    [property: JsonPropertyName("max_merchant_avg_amount")] double MaxMerchantAvgAmount);
