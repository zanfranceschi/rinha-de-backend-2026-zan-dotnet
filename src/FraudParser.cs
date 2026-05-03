using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Rinha2026.Services;

public struct FraudData
{
    public double TransactionAmount;
    public int Installments;
    public DateTime RequestedAt;
    public double CustomerAvgAmount;
    public int TxCount24h;
    public bool IsKnownMerchant;
    public string MerchantMcc;
    public double MerchantAvgAmount;
    public bool IsOnline;
    public bool CardPresent;
    public double KmFromHome;
    public bool HasLastTransaction;
    public DateTime LastTimestamp;
    public double LastKmFromCurrent;
}

public static class FraudParser
{
    public static FraudData Parse(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        var data = new FraudData();

        Span<byte> merchantIdBuf = stackalloc byte[128];
        int merchantIdLen = -1;

        Span<byte> kmBuf = stackalloc byte[2048];
        int kmBufOffset = 0;
        Span<int> kmOffsets = stackalloc int[32];
        Span<int> kmLens = stackalloc int[32];
        int kmCount = -1;

        reader.Read(); // StartObject

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            if (reader.ValueTextEquals("transaction"u8))
            {
                reader.Read(); // StartObject
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("amount"u8))
                    {
                        reader.Read();
                        data.TransactionAmount = reader.GetDouble();
                    }
                    else if (reader.ValueTextEquals("installments"u8))
                    {
                        reader.Read();
                        data.Installments = reader.GetInt32();
                    }
                    else if (reader.ValueTextEquals("requested_at"u8))
                    {
                        reader.Read();
                        data.RequestedAt = ParseIsoUtc(reader.ValueSpan);
                    }
                    else
                        reader.Skip();
                }
            }
            else if (reader.ValueTextEquals("customer"u8))
            {
                reader.Read(); // StartObject
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("avg_amount"u8))
                    {
                        reader.Read();
                        data.CustomerAvgAmount = reader.GetDouble();
                    }
                    else if (reader.ValueTextEquals("tx_count_24h"u8))
                    {
                        reader.Read();
                        data.TxCount24h = reader.GetInt32();
                    }
                    else if (reader.ValueTextEquals("known_merchants"u8))
                    {
                        reader.Read(); // StartArray
                        if (merchantIdLen >= 0)
                        {
                            var mid = merchantIdBuf[..merchantIdLen];
                            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                            {
                                if (reader.ValueTextEquals(mid))
                                    data.IsKnownMerchant = true;
                            }
                            kmCount = 0;
                        }
                        else
                        {
                            kmCount = 0;
                            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                            {
                                if (kmCount < 32)
                                {
                                    int len = reader.CopyString(kmBuf[kmBufOffset..]);
                                    kmOffsets[kmCount] = kmBufOffset;
                                    kmLens[kmCount] = len;
                                    kmBufOffset += len;
                                    kmCount++;
                                }
                            }
                        }
                    }
                    else
                        reader.Skip();
                }
            }
            else if (reader.ValueTextEquals("merchant"u8))
            {
                reader.Read(); // StartObject
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("id"u8))
                    {
                        reader.Read();
                        merchantIdLen = reader.CopyString(merchantIdBuf);
                    }
                    else if (reader.ValueTextEquals("mcc"u8))
                    {
                        reader.Read();
                        data.MerchantMcc = reader.GetString()!;
                    }
                    else if (reader.ValueTextEquals("avg_amount"u8))
                    {
                        reader.Read();
                        data.MerchantAvgAmount = reader.GetDouble();
                    }
                    else
                        reader.Skip();
                }

                // Resolve deferred known_merchants
                if (kmCount > 0 && merchantIdLen >= 0)
                {
                    var mid = merchantIdBuf[..merchantIdLen];
                    for (int i = 0; i < kmCount; i++)
                    {
                        if (mid.SequenceEqual(kmBuf.Slice(kmOffsets[i], kmLens[i])))
                        {
                            data.IsKnownMerchant = true;
                            break;
                        }
                    }
                }
            }
            else if (reader.ValueTextEquals("terminal"u8))
            {
                reader.Read(); // StartObject
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("is_online"u8))
                    {
                        reader.Read();
                        data.IsOnline = reader.GetBoolean();
                    }
                    else if (reader.ValueTextEquals("card_present"u8))
                    {
                        reader.Read();
                        data.CardPresent = reader.GetBoolean();
                    }
                    else if (reader.ValueTextEquals("km_from_home"u8))
                    {
                        reader.Read();
                        data.KmFromHome = reader.GetDouble();
                    }
                    else
                        reader.Skip();
                }
            }
            else if (reader.ValueTextEquals("last_transaction"u8))
            {
                reader.Read(); // value or StartObject
                if (reader.TokenType == JsonTokenType.StartObject)
                {
                    data.HasLastTransaction = true;
                    while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                    {
                        if (reader.ValueTextEquals("timestamp"u8))
                        {
                            reader.Read();
                            data.LastTimestamp = ParseIsoUtc(reader.ValueSpan);
                        }
                        else if (reader.ValueTextEquals("km_from_current"u8))
                        {
                            reader.Read();
                            data.LastKmFromCurrent = reader.GetDouble();
                        }
                        else
                            reader.Skip();
                    }
                }
            }
            else
                reader.Skip();
        }

        return data;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTime ParseIsoUtc(ReadOnlySpan<byte> s)
    {
        int y = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        int M = (s[5] - '0') * 10 + (s[6] - '0');
        int d = (s[8] - '0') * 10 + (s[9] - '0');
        int h = (s[11] - '0') * 10 + (s[12] - '0');
        int m = (s[14] - '0') * 10 + (s[15] - '0');
        int sec = (s[17] - '0') * 10 + (s[18] - '0');
        return new DateTime(y, M, d, h, m, sec, DateTimeKind.Utc);
    }
}
