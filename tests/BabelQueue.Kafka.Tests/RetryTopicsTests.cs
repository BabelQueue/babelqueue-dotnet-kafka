using System;
using BabelQueue;
using BabelQueue.Kafka;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>§6.4–§6.5 retry/delay topology: tier naming, delay→tier, attempt→tier, raises, DLQ.</summary>
public sealed class RetryTopicsTests
{
    [Fact]
    public void TiersAreNamedAndSortedAscending()
    {
        var rt = RetryTopics.ForTopic("orders")
            .Tier(TimeSpan.FromMinutes(5))
            .Tier(TimeSpan.FromSeconds(5))
            .Tier(TimeSpan.FromSeconds(30))
            .Build();

        Assert.Equal("orders.dlq", rt.DlqTopic);
        Assert.True(rt.HasTiers);
        Assert.Equal("orders.retry.1", rt.Tiers[0].Topic);
        Assert.Equal(TimeSpan.FromSeconds(5), rt.Tiers[0].Delay);
        Assert.Equal("orders.retry.2", rt.Tiers[1].Topic);
        Assert.Equal("orders.retry.3", rt.Tiers[2].Topic);
    }

    [Fact]
    public void TierForDelayPicksSmallestSufficient()
    {
        var rt = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Tier(TimeSpan.FromSeconds(30)).Build();
        Assert.Equal("orders.retry.1", rt.TierForDelay(TimeSpan.FromSeconds(3)).Topic);
        Assert.Equal("orders.retry.1", rt.TierForDelay(TimeSpan.FromSeconds(5)).Topic);
        Assert.Equal("orders.retry.2", rt.TierForDelay(TimeSpan.FromSeconds(10)).Topic);
    }

    [Fact]
    public void TierForDelayTooLargeRaises()
    {
        var rt = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Build();
        Assert.Throws<BabelQueueException>(() => rt.TierForDelay(TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void TierForAttemptClampsToLast()
    {
        var rt = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Tier(TimeSpan.FromSeconds(30)).Build();
        Assert.Equal("orders.retry.1", rt.TierForAttempt(0).Topic);
        Assert.Equal("orders.retry.2", rt.TierForAttempt(1).Topic);
        Assert.Equal("orders.retry.2", rt.TierForAttempt(9).Topic);
    }

    [Fact]
    public void NoTiersRaisesAndDlqToggles()
    {
        var rt = RetryTopics.ForTopic("orders").Build();
        Assert.False(rt.HasTiers);
        Assert.Throws<BabelQueueException>(() => rt.TierForAttempt(0));
        Assert.Null(RetryTopics.ForTopic("orders").WithoutDlq().Build().DlqTopic);
        Assert.Equal("orders-dead", RetryTopics.ForTopic("orders").DlqTopic("orders-dead").Build().DlqTopic);
    }

    [Fact]
    public void RecommendedTiersAreAnExplicitOptIn()
    {
        Assert.Equal(new[] { 5_000, 30_000, 300_000, 1_800_000 }, RetryTopics.RecommendedTierMillis);

        var rt = RetryTopics.ForTopic("orders").RecommendedTiers().Build();
        Assert.Equal(4, rt.Tiers.Count);
        Assert.Equal(TimeSpan.FromSeconds(5), rt.Tiers[0].Delay);
        Assert.Equal(TimeSpan.FromMinutes(30), rt.Tiers[3].Delay);
        Assert.Equal("orders.retry.4", rt.Tiers[3].Topic);

        // The default topology is unchanged: no tiers unless asked for.
        Assert.False(RetryTopics.ForTopic("orders").Build().HasTiers);
    }

    [Fact]
    public void TierForDelayNeverClampsDownToTheLargestTier()
    {
        // Delay publish (ADR-0034 §3): smallest tier >= delay, an over-large delay raises — it is not clamped.
        var rt = RetryTopics.ForTopic("orders").RecommendedTiers().Build();
        Assert.Equal("orders.retry.4", rt.TierForDelay(TimeSpan.FromMinutes(30)).Topic);
        Assert.Throws<BabelQueueException>(() => rt.TierForDelay(TimeSpan.FromMinutes(30) + TimeSpan.FromMilliseconds(1)));
        // ...while the retry rule does clamp: attempts beyond the tier count stay on the last tier.
        Assert.Equal("orders.retry.4", rt.TierForAttempt(100).Topic);
    }
}
